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

//! Row-local, infallible expressions that runtime pruning may bypass.
//!
//! This policy is independent of a reader backend and of predicate translation.
//! New expression support belongs here with proofs for nulls, types and errors.

use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, Literal,
};
use datafusion::physical_expr::PhysicalExpr;
use std::sync::Arc;

/// Whether skipping input rows preserves both expression values and evaluation errors.
///
/// Determinism alone is insufficient: a deterministic cast, division, or UDF can
/// fail on a row that the runtime filter would discard. Function volatility does
/// not promise infallibility either. Admit only known safe expression shapes;
/// unfamiliar expressions keep their original evaluation boundary.
pub(super) fn is_safe_to_prune_before(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &arrow::datatypes::Schema,
) -> bool {
    use arrow::datatypes::DataType;
    use datafusion::physical_expr::expressions::NotExpr;

    let safe_node = if expr.is::<Column>() || expr.is::<Literal>() {
        true
    } else if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        match binary.op() {
            Operator::And | Operator::Or => {
                binary.left().data_type(schema).ok() == Some(DataType::Boolean)
                    && binary.right().data_type(schema).ok() == Some(DataType::Boolean)
            }
            Operator::Eq
            | Operator::NotEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom => {
                // Avoid implicit casts or unsupported kernels during evaluation.
                let left = binary.left().data_type(schema).ok();
                left.as_ref().is_some_and(is_comparable_type)
                    && left == binary.right().data_type(schema).ok()
            }
            Operator::Divide | Operator::Modulo => {
                // Signed integer division/remainder cannot overflow or divide by
                // zero when the divisor is a nonzero constant other than -1.
                let Some(literal) = binary.right().downcast_ref::<Literal>() else {
                    return false;
                };
                is_safe_integer_divisor(literal.value())
                    && binary.left().data_type(schema).ok() == Some(literal.value().data_type())
            }
            _ => false,
        }
    } else if let Some(list) = expr.downcast_ref::<InListExpr>() {
        let data_type = list.expr().data_type(schema).ok();
        data_type.as_ref().is_some_and(is_comparable_type)
            && list.list().iter().all(|item| {
                item.downcast_ref::<Literal>()
                    .is_some_and(|literal| Some(literal.value().data_type()) == data_type)
            })
    } else if let Some(not) = expr.downcast_ref::<NotExpr>() {
        not.arg().data_type(schema).ok() == Some(DataType::Boolean)
    } else {
        expr.is::<IsNullExpr>() || expr.is::<IsNotNullExpr>()
    };
    safe_node
        && expr
            .children()
            .iter()
            .all(|child| is_safe_to_prune_before(child, schema))
}

fn is_safe_integer_divisor(value: &ScalarValue) -> bool {
    let divisor = match value {
        ScalarValue::Int8(Some(value)) => i64::from(*value),
        ScalarValue::Int16(Some(value)) => i64::from(*value),
        ScalarValue::Int32(Some(value)) => i64::from(*value),
        ScalarValue::Int64(Some(value)) => *value,
        _ => return false,
    };
    divisor != 0 && divisor != -1
}

fn is_comparable_type(data_type: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::DataType;

    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_expr::expressions::lit;
    #[test]
    fn reader_attachment_passes_row_local_infallible_filters() {
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_expr::expressions::NotExpr;
        use datafusion_comet_spark_expr::RandExpr;

        let schema = Schema::new(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("value", DataType::Int32, true),
        ]);
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let value: Arc<dyn PhysicalExpr> = Arc::new(Column::new("value", 1));
        let modulo: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::clone(&value),
                Operator::Modulo,
                lit(3_i32),
            )),
            Operator::Eq,
            lit(0_i32),
        ));
        let deterministic: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(NotExpr::new(Arc::new(IsNullExpr::new(Arc::clone(&key))))),
            Operator::And,
            Arc::clone(&modulo),
        ));
        assert!(is_safe_to_prune_before(&deterministic, &schema));

        // A volatile expression anywhere below the filter keeps it in the way.
        let random: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(RandExpr::new(42)),
            Operator::Lt,
            lit(0.5_f64),
        ));
        let mixed: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(modulo, Operator::And, random));
        assert!(!is_safe_to_prune_before(&mixed, &schema));
    }

    #[test]
    fn deterministic_expressions_with_evaluation_errors_remain_boundaries() {
        use arrow::array::{Int32Array, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_expr::expressions::CastExpr;

        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![i32::MIN, i32::MAX])),
                Arc::new(StringArray::from(vec!["bad", "1"])),
            ],
        )
        .unwrap();
        let value: Arc<dyn PhysicalExpr> = Arc::new(Column::new("value", 0));
        let expressions: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(CastExpr::new(
                Arc::new(Column::new("text", 1)),
                DataType::Int32,
                None,
            )),
            Arc::new(BinaryExpr::new(
                Arc::clone(&value),
                Operator::Divide,
                lit(0_i32),
            )),
            Arc::new(BinaryExpr::new(
                Arc::clone(&value),
                Operator::Divide,
                lit(-1_i32),
            )),
            Arc::new(
                BinaryExpr::new(value, Operator::Plus, lit(1_i32)).with_fail_on_overflow(true),
            ),
        ];
        for expression in expressions {
            assert!(expression.evaluate(&batch).is_err(), "{expression}");
            assert!(
                !is_safe_to_prune_before(&expression, schema.as_ref()),
                "{expression} must be evaluated before pruning"
            );
        }
    }

    #[test]
    fn division_safety_requires_a_matching_nonzero_constant_divisor() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Schema::new(vec![Field::new("key", DataType::Int32, false)]);
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        for operation in [Operator::Divide, Operator::Modulo] {
            for divisor in [-3_i32, 2, i32::MAX] {
                let expression: Arc<dyn PhysicalExpr> =
                    Arc::new(BinaryExpr::new(Arc::clone(&key), operation, lit(divisor)));
                assert!(is_safe_to_prune_before(&expression, &schema));
            }
            for divisor in [lit(0_i32), lit(-1_i32), lit(3_i64), Arc::clone(&key)] {
                let expression: Arc<dyn PhysicalExpr> =
                    Arc::new(BinaryExpr::new(Arc::clone(&key), operation, divisor));
                assert!(!is_safe_to_prune_before(&expression, &schema));
            }
        }
    }
}
