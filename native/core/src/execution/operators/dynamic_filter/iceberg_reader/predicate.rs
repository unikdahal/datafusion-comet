// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Conservative translation of producer predicates to Iceberg predicates.
//! Unsupported conjuncts may be omitted; unsupported disjunctions fail open.

use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, Literal,
};
use datafusion::physical_expr::PhysicalExpr;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, Type};
use std::sync::Arc;

pub(super) fn extract_iceberg_predicate(
    expr: &Arc<dyn PhysicalExpr>,
    probe_column_index: usize,
    iceberg_field_name: &str,
) -> Option<Predicate> {
    // An AND permits a conservative subset of its constraints. Do not descend
    // through arbitrary OR, NOT, casts, or exact hash membership.
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        if binary.op() == &Operator::And {
            return match (
                extract_iceberg_predicate(binary.left(), probe_column_index, iceberg_field_name),
                extract_iceberg_predicate(binary.right(), probe_column_index, iceberg_field_name),
            ) {
                (Some(left), Some(right)) => Some(left.and(right)),
                (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
                (None, None) => None,
            };
        }
        if binary.op() == &Operator::Or {
            return extract_disjunction(binary, probe_column_index, iceberg_field_name);
        }
        return extract_bound(binary, probe_column_index, iceberg_field_name);
    }

    if let Some(list) = expr.downcast_ref::<InListExpr>() {
        return extract_in_list(list, probe_column_index, iceberg_field_name);
    }

    if let Some(check) = expr.downcast_ref::<IsNullExpr>() {
        return (check.arg().downcast_ref::<Column>()?.index() == probe_column_index)
            .then(|| Reference::new(iceberg_field_name).is_null());
    }
    if let Some(check) = expr.downcast_ref::<IsNotNullExpr>() {
        return (check.arg().downcast_ref::<Column>()?.index() == probe_column_index)
            .then(|| Reference::new(iceberg_field_name).is_not_null());
    }

    expr.downcast_ref::<Literal>()
        .and_then(|literal| match literal.value() {
            ScalarValue::Boolean(Some(false)) => Some(Predicate::AlwaysFalse),
            _ => None,
        })
}

/// The largest `IN` list translated into an Iceberg predicate. DataFusion only publishes a list
/// for small build sides; larger ones stay hash lookups on the probe side.
pub(super) const MAX_IN_LIST_LITERALS: usize = 1024;

/// `key IN (v1, v2, ...)` over the probe column, or `None` if any part is not a plain literal.
fn extract_in_list(
    list: &InListExpr,
    probe_column_index: usize,
    iceberg_field_name: &str,
) -> Option<Predicate> {
    if list.negated() || list.is_empty() || list.len() > MAX_IN_LIST_LITERALS {
        return None;
    }
    if list.expr().downcast_ref::<Column>()?.index() != probe_column_index {
        return None;
    }
    let datums = list
        .list()
        .iter()
        .map(|item| {
            item.downcast_ref::<Literal>()
                .and_then(|literal| scalar_to_datum(literal.value()))
        })
        .collect::<Option<Vec<_>>>()?;
    Some(Reference::new(iceberg_field_name).is_in(datums))
}

/// A leaf of `a OR b OR ...` with nested ORs flattened left to right.
fn flatten_or<'a>(expr: &'a Arc<dyn PhysicalExpr>, arms: &mut Vec<&'a Arc<dyn PhysicalExpr>>) {
    match expr.downcast_ref::<BinaryExpr>() {
        Some(binary) if binary.op() == &Operator::Or => {
            flatten_or(binary.left(), arms);
            flatten_or(binary.right(), arms);
        }
        _ => arms.push(expr),
    }
}

/// Whether an expression evaluates to literal boolean false.
fn is_literal_false(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.downcast_ref::<Literal>()
        .is_some_and(|lit| matches!(lit.value(), ScalarValue::Boolean(Some(false))))
}

/// Whether the conjunct provably implies `column IS NULL` on the probe column.
fn arm_implies_null(expr: &Arc<dyn PhysicalExpr>, probe_column_index: usize) -> bool {
    if let Some(null) = expr.downcast_ref::<IsNullExpr>() {
        return null
            .arg()
            .downcast_ref::<Column>()
            .is_some_and(|col| col.index() == probe_column_index);
    }
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        match binary.op() {
            Operator::And => {
                arm_implies_null(binary.left(), probe_column_index)
                    || arm_implies_null(binary.right(), probe_column_index)
            }
            Operator::Or => {
                if is_literal_false(binary.left()) {
                    arm_implies_null(binary.right(), probe_column_index)
                } else if is_literal_false(binary.right()) {
                    arm_implies_null(binary.left(), probe_column_index)
                } else {
                    arm_implies_null(binary.left(), probe_column_index)
                        && arm_implies_null(binary.right(), probe_column_index)
                }
            }
            Operator::Eq => {
                let left_col = binary
                    .left()
                    .downcast_ref::<Column>()
                    .is_some_and(|column| column.index() == probe_column_index);
                let right_col = binary
                    .right()
                    .downcast_ref::<Column>()
                    .is_some_and(|column| column.index() == probe_column_index);
                let left_null = binary
                    .left()
                    .downcast_ref::<Literal>()
                    .is_some_and(|value| value.value().is_null());
                let right_null = binary
                    .right()
                    .downcast_ref::<Literal>()
                    .is_some_and(|value| value.value().is_null());
                (left_col && right_null) || (right_col && left_null)
            }
            _ => false,
        }
    } else {
        false
    }
}

/// Whether the leftmost conjunct of `arm` is `column = literal` on the probe column.
fn starts_with_equality(
    arm: &Arc<dyn PhysicalExpr>,
    probe_column_index: usize,
    literal: &ScalarValue,
) -> bool {
    let Some(binary) = arm.downcast_ref::<BinaryExpr>() else {
        return false;
    };
    match binary.op() {
        Operator::And => starts_with_equality(binary.left(), probe_column_index, literal),
        Operator::Eq => {
            (binary
                .left()
                .downcast_ref::<Column>()
                .is_some_and(|column| column.index() == probe_column_index)
                && binary
                    .right()
                    .downcast_ref::<Literal>()
                    .is_some_and(|value| value.value() == literal))
                || (binary
                    .right()
                    .downcast_ref::<Column>()
                    .is_some_and(|column| column.index() == probe_column_index)
                    && binary
                        .left()
                        .downcast_ref::<Literal>()
                        .is_some_and(|value| value.value() == literal))
        }
        _ => false,
    }
}

/// The OR shapes DataFusion's TopK produces, translated on the first sort key only:
///
/// * single key: `key < t` or `key IS NULL OR key < t` (also `>`), kept exact;
/// * several keys: those arms followed by one `key = t AND ...` arm per further key. Every
///   further arm implies `key = t`, so the first key alone is bounded by `key <= t`
///   (`>=` for descending), plus `OR key IS NULL` when nulls sort first;
/// * nulls-first all-null heap: DataFusion publishes
///   `false OR ((key IS NULL OR key = NULL) AND ...)` where every arm implies `key IS NULL`.
///   This is safely translated to `is_null(key)`.
///
/// Any other OR fails open.
fn extract_disjunction(
    binary: &BinaryExpr,
    probe_column_index: usize,
    iceberg_field_name: &str,
) -> Option<Predicate> {
    let mut arms = Vec::new();
    flatten_or(binary.left(), &mut arms);
    flatten_or(binary.right(), &mut arms);

    let arms: Vec<_> = arms
        .into_iter()
        .filter(|arm| !is_literal_false(arm))
        .collect();

    if arms.is_empty() {
        return Some(Predicate::AlwaysFalse);
    }

    if arms
        .iter()
        .all(|arm| arm_implies_null(arm, probe_column_index))
    {
        return Some(Reference::new(iceberg_field_name).is_null());
    }

    let (null_arms, value_arms): (Vec<_>, Vec<_>) = arms
        .into_iter()
        .partition(|arm| arm_implies_null(arm, probe_column_index));

    let with_null = !null_arms.is_empty();

    let comparison = value_arms.first()?.downcast_ref::<BinaryExpr>()?;
    if !matches!(comparison.op(), Operator::Lt | Operator::Gt) {
        return None;
    }
    let column = comparison.left().downcast_ref::<Column>()?;
    if column.index() != probe_column_index {
        return None;
    }
    let threshold = comparison.right().downcast_ref::<Literal>()?;
    let rest = &value_arms[1..];
    let bound = if rest.is_empty() {
        extract_bound(comparison, probe_column_index, iceberg_field_name)?
    } else {
        if !rest
            .iter()
            .all(|arm| starts_with_equality(arm, probe_column_index, threshold.value()))
        {
            return None;
        }
        let datum = scalar_to_datum(threshold.value())?;
        let reference = Reference::new(iceberg_field_name);
        match comparison.op() {
            Operator::Lt => reference.less_than_or_equal_to(datum),
            _ => reference.greater_than_or_equal_to(datum),
        }
    };
    Some(if with_null {
        Reference::new(iceberg_field_name).is_null().or(bound)
    } else {
        bound
    })
}

fn extract_bound(
    binary: &BinaryExpr,
    probe_column_index: usize,
    iceberg_field_name: &str,
) -> Option<Predicate> {
    let column = binary.left().downcast_ref::<Column>()?;
    if column.index() != probe_column_index {
        return None;
    }
    let literal = binary.right().downcast_ref::<Literal>()?;
    let datum = scalar_to_datum(literal.value())?;
    let reference = Reference::new(iceberg_field_name);

    match binary.op() {
        Operator::Gt => Some(reference.greater_than(datum)),
        Operator::Lt => Some(reference.less_than(datum)),
        Operator::GtEq => Some(reference.greater_than_or_equal_to(datum)),
        Operator::LtEq => Some(reference.less_than_or_equal_to(datum)),
        _ => None,
    }
}

/// Converts a runtime pruning scalar to an Iceberg Datum.
/// Supported variants correspond to [`crate::execution::operators::dynamic_filter::is_runtime_pruning_key_type`].
fn scalar_to_datum(value: &ScalarValue) -> Option<Datum> {
    match value {
        ScalarValue::Int8(Some(value)) => Some(Datum::int(i32::from(*value))),
        ScalarValue::Int16(Some(value)) => Some(Datum::int(i32::from(*value))),
        ScalarValue::Int32(Some(value)) => Some(Datum::int(*value)),
        ScalarValue::Int64(Some(value)) => Some(Datum::long(*value)),
        ScalarValue::Date32(Some(days)) => Some(Datum::date(*days)),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Some(Datum::string(value.as_str())),
        ScalarValue::Dictionary(_, value)
            if matches!(
                value.as_ref(),
                ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_) | ScalarValue::Utf8View(_)
            ) =>
        {
            scalar_to_datum(value)
        }
        ScalarValue::Decimal128(Some(value), precision, scale) => {
            let scale = u32::try_from(*scale).ok()?;
            if scale > u32::from(*precision) {
                return None;
            }
            let decimal_type = Type::decimal(u32::from(*precision), scale).ok()?;
            if value.unsigned_abs() >= 10_u128.pow(u32::from(*precision)) {
                return None;
            }
            // Iceberg decimals serialize the unscaled i128 in signed big-endian form.
            // Validate precision above before using the existing public byte constructor.
            // The mantissa and scale are unchanged: no parsing, rounding or rescaling.
            Datum::try_from_bytes(
                &value.to_be_bytes(),
                decimal_type.as_primitive_type()?.clone(),
            )
            .ok()?
            .to(&decimal_type)
            .ok()
        }
        // Spark TIMESTAMP carries a zone and maps to Iceberg timestamptz; TIMESTAMP_NTZ has
        // none and maps to timestamp. Both store microseconds since the epoch.
        ScalarValue::TimestampMicrosecond(Some(micros), Some(_)) => {
            Some(Datum::timestamptz_micros(*micros))
        }
        ScalarValue::TimestampMicrosecond(Some(micros), None) => {
            Some(Datum::timestamp_micros(*micros))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::PrimitiveLiteral;

    #[test]
    fn string_scalars_preserve_utf8_bytes_and_exclude_nulls() {
        for value in [
            "".to_owned(),
            "ascii".to_owned(),
            "é東京🙂".to_owned(),
            "prefix".repeat(4096),
        ] {
            for scalar in [
                ScalarValue::Utf8(Some(value.clone())),
                ScalarValue::LargeUtf8(Some(value.clone())),
                ScalarValue::Utf8View(Some(value.clone())),
            ] {
                assert_eq!(scalar_to_datum(&scalar), Some(Datum::string(&value)));
                let dictionary = ScalarValue::Dictionary(
                    Box::new(arrow::datatypes::DataType::Int32),
                    Box::new(scalar),
                );
                assert_eq!(scalar_to_datum(&dictionary), Some(Datum::string(&value)));
            }
        }
        for scalar in [
            ScalarValue::Utf8(None),
            ScalarValue::LargeUtf8(None),
            ScalarValue::Utf8View(None),
        ] {
            assert!(scalar_to_datum(&scalar).is_none());
            let dictionary = ScalarValue::Dictionary(
                Box::new(arrow::datatypes::DataType::Int32),
                Box::new(scalar),
            );
            assert!(scalar_to_datum(&dictionary).is_none());
        }
    }

    #[test]
    fn string_thresholds_preserve_strictness_and_full_literal() {
        use datafusion::physical_expr::expressions::lit;

        for value in [
            "",
            "é東京🙂",
            "abcdefghijklmnop000000",
            "abcdefghijklmnop999999",
        ] {
            for scalar in [
                ScalarValue::Utf8(Some(value.into())),
                ScalarValue::LargeUtf8(Some(value.into())),
                ScalarValue::Utf8View(Some(value.into())),
            ] {
                for literal in [
                    scalar.clone(),
                    ScalarValue::Dictionary(
                        Box::new(arrow::datatypes::DataType::Int32),
                        Box::new(scalar),
                    ),
                ] {
                    for (op, expected) in [
                        (
                            Operator::Lt,
                            Reference::new("id").less_than(Datum::string(value)),
                        ),
                        (
                            Operator::Gt,
                            Reference::new("id").greater_than(Datum::string(value)),
                        ),
                        (
                            Operator::LtEq,
                            Reference::new("id").less_than_or_equal_to(Datum::string(value)),
                        ),
                        (
                            Operator::GtEq,
                            Reference::new("id").greater_than_or_equal_to(Datum::string(value)),
                        ),
                    ] {
                        let expression: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("key", 0)),
                            op,
                            lit(literal.clone()),
                        ));
                        assert_eq!(
                            extract_iceberg_predicate(&expression, 0, "id"),
                            Some(expected)
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn arrow_and_iceberg_strings_share_unsigned_byte_order() {
        use arrow::array::{make_comparator, StringArray};
        use arrow::compute::cast;
        use arrow::compute::SortOptions;
        use arrow::datatypes::DataType;

        let values = [
            "",
            "A",
            "a",
            "abcdefghijklmnop000000",
            "abcdefghijklmnop999999",
            "abcdefghijklmnoq",
            "é",
            "東京",
            "🙂",
        ];
        let strings = StringArray::from(values.to_vec());
        for value_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            for data_type in [
                value_type.clone(),
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(value_type)),
            ] {
                let array = cast(&strings, &data_type).unwrap();
                for descending in [false, true] {
                    let cmp = make_comparator(
                        array.as_ref(),
                        array.as_ref(),
                        SortOptions {
                            descending,
                            nulls_first: false,
                        },
                    )
                    .unwrap();
                    for (i, left) in values.iter().enumerate() {
                        for (j, right) in values.iter().enumerate() {
                            let bytes = left.as_bytes().cmp(right.as_bytes());
                            let datum = Datum::string(left)
                                .partial_cmp(&Datum::string(right))
                                .unwrap();
                            assert_eq!(datum, bytes);
                            assert_eq!(cmp(i, j), if descending { bytes.reverse() } else { bytes });
                        }
                    }
                }
            }
        }
        // Iceberg truncate(16) conservatively encloses both long suffixes.
        let lower = Datum::string("abcdefghijklmnop");
        let upper = Datum::string("abcdefghijklmnoq");
        for actual in ["abcdefghijklmnop000000", "abcdefghijklmnop999999"] {
            let actual = Datum::string(actual);
            assert!(lower <= actual && actual <= upper);
        }
    }

    #[test]
    fn decimal_scalars_preserve_unscaled_value_precision_and_scale() {
        let maximum = 10_i128.pow(38) - 1;
        for (value, precision, scale) in [
            (12345, 18, 2),
            (-12345, 18, 2),
            (0, 18, 2),
            (maximum, 38, 0),
            (-maximum, 38, 38),
            (1, 38, 38),
        ] {
            let datum =
                scalar_to_datum(&ScalarValue::Decimal128(Some(value), precision, scale)).unwrap();
            assert_eq!(datum.literal(), &PrimitiveLiteral::Int128(value));
            assert_eq!(
                datum.data_type(),
                Type::decimal(u32::from(precision), scale as u32)
                    .unwrap()
                    .as_primitive_type()
                    .unwrap()
            );
        }
    }

    #[test]
    fn decimal_scalars_reject_null_invalid_types_and_precision_overflow() {
        for value in [
            ScalarValue::Decimal128(None, 18, 2),
            ScalarValue::Decimal128(Some(1), 18, -1),
            ScalarValue::Decimal128(Some(1), 18, 19),
            ScalarValue::Decimal128(Some(1), 0, 0),
            ScalarValue::Decimal128(Some(1), 39, 2),
            ScalarValue::Decimal128(Some(1000), 3, 2),
            ScalarValue::Decimal128(Some(-1000), 3, 2),
            ScalarValue::Decimal128(Some(i128::MIN), 38, 2),
            ScalarValue::Decimal128(Some(i128::MAX), 38, 2),
        ] {
            assert!(scalar_to_datum(&value).is_none(), "{value:?}");
        }
    }

    #[test]
    fn decimal_datum_rejects_scale_mismatch_and_narrow_precision() {
        let datum = scalar_to_datum(&ScalarValue::Decimal128(Some(12345), 18, 2)).unwrap();
        assert!(datum.clone().to(&Type::decimal(18, 3).unwrap()).is_err());
        assert!(datum.clone().to(&Type::decimal(4, 2).unwrap()).is_err());
        assert_eq!(
            datum.to(&Type::decimal(10, 2).unwrap()).unwrap().literal(),
            &PrimitiveLiteral::Int128(12345)
        );
    }
}
