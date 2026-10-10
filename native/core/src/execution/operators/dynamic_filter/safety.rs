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
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion_comet_spark_expr::IfExpr;
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
    } else if let Some(func) = expr.downcast_ref::<ScalarFunctionExpr>() {
        is_safe_spark_modulo(func, schema)
    } else if let Some(if_expr) = expr.downcast_ref::<IfExpr>() {
        let children = if_expr.children();
        children.len() == 3 && children[0].data_type(schema).ok() == Some(DataType::Boolean)
    } else {
        expr.is::<IsNullExpr>() || expr.is::<IsNotNullExpr>()
    };
    safe_node
        && expr
            .children()
            .iter()
            .all(|child| is_safe_to_prune_before(child, schema))
}

fn is_safe_spark_modulo(func: &ScalarFunctionExpr, schema: &arrow::datatypes::Schema) -> bool {
    if func.name() != "spark_modulo" && func.fun().name() != "spark_modulo" {
        return false;
    }
    let args = func.args();
    if args.len() != 2 {
        return false;
    }
    let left_type = args[0].data_type(schema).ok();
    if !left_type.as_ref().is_some_and(is_comparable_type) {
        return false;
    }
    if left_type != args[1].data_type(schema).ok() {
        return false;
    }

    let divisor = &args[1];
    // In non-ANSI mode, Comet wraps the divisor with `null_if_zero_primitive` (an `IfExpr`),
    // replacing zero with NULL so modulo evaluates to NULL without division-by-zero error.
    // In ANSI mode, modulo can error on zero divisor; only a constant nonzero literal other
    // than -1 is infallible.
    is_non_ansi_zero_guard(divisor)
        || divisor
            .downcast_ref::<Literal>()
            .is_some_and(|lit| is_safe_integer_divisor(lit.value()))
}

fn is_non_ansi_zero_guard(expr: &Arc<dyn PhysicalExpr>) -> bool {
    let Some(if_expr) = expr.downcast_ref::<IfExpr>() else {
        return false;
    };
    let children = if_expr.children();
    if children.len() != 3 {
        return false;
    }
    let Some(true_lit) = children[1].downcast_ref::<Literal>() else {
        return false;
    };
    if !true_lit.value().is_null() {
        return false;
    }
    let Some(binary) = children[0].downcast_ref::<BinaryExpr>() else {
        return false;
    };
    if binary.op() != &Operator::Eq {
        return false;
    }
    let Some(zero_lit) = binary.right().downcast_ref::<Literal>() else {
        return false;
    };
    if !is_zero(zero_lit.value()) {
        return false;
    }
    if let Some(lit) = children[2].downcast_ref::<Literal>() {
        if is_minus_one(lit.value()) {
            return false;
        }
    }
    true
}

fn is_zero(value: &ScalarValue) -> bool {
    use arrow::datatypes::i256;

    match value {
        ScalarValue::Int8(Some(0))
        | ScalarValue::Int16(Some(0))
        | ScalarValue::Int32(Some(0))
        | ScalarValue::Int64(Some(0))
        | ScalarValue::UInt8(Some(0))
        | ScalarValue::UInt16(Some(0))
        | ScalarValue::UInt32(Some(0))
        | ScalarValue::UInt64(Some(0)) => true,
        ScalarValue::Float32(Some(v)) => *v == 0.0,
        ScalarValue::Float64(Some(v)) => *v == 0.0,
        ScalarValue::Decimal128(Some(0), _, _) => true,
        ScalarValue::Decimal256(Some(v), _, _) => *v == i256::from(0),
        _ => false,
    }
}

fn is_minus_one(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Int8(Some(-1))
            | ScalarValue::Int16(Some(-1))
            | ScalarValue::Int32(Some(-1))
            | ScalarValue::Int64(Some(-1))
    )
}

fn is_safe_integer_divisor(value: &ScalarValue) -> bool {
    let divisor = match value {
        ScalarValue::Int8(Some(value)) => i64::from(*value),
        ScalarValue::Int16(Some(value)) => i64::from(*value),
        ScalarValue::Int32(Some(value)) => i64::from(*value),
        ScalarValue::Int64(Some(value)) => *value,
        ScalarValue::UInt8(Some(value)) => i64::from(*value),
        ScalarValue::UInt16(Some(value)) => i64::from(*value),
        ScalarValue::UInt32(Some(value)) => i64::from(*value),
        ScalarValue::UInt64(Some(value)) => return *value != 0,
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

    #[test]
    fn spark_modulo_safety_gates_on_ansi_and_divisor() {
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::execution::context::SessionContext;
        use datafusion_comet_spark_expr::{create_modulo_expr, RandExpr};

        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new("divisor", DataType::Int32, false),
        ]));
        let value: Arc<dyn PhysicalExpr> = Arc::new(Column::new("value", 0));
        let divisor: Arc<dyn PhysicalExpr> = Arc::new(Column::new("divisor", 1));
        let session = SessionContext::new();

        // 1. spark_modulo with nonzero constant divisor is safe in both ANSI and non-ANSI
        for fail_on_error in [true, false] {
            let expr = create_modulo_expr(
                Arc::clone(&value),
                lit(3_i32),
                DataType::Int32,
                Arc::clone(&schema),
                fail_on_error,
                &session.state(),
            )
            .unwrap();
            assert!(
                is_safe_to_prune_before(&expr, schema.as_ref()),
                "spark_modulo by non-zero literal should be safe (fail_on_error={fail_on_error})"
            );
        }

        // 2. Non-ANSI modulo by column is safe because divisor zero is wrapped to NULL
        let non_ansi_col = create_modulo_expr(
            Arc::clone(&value),
            Arc::clone(&divisor),
            DataType::Int32,
            Arc::clone(&schema),
            false,
            &session.state(),
        )
        .unwrap();
        assert!(
            is_safe_to_prune_before(&non_ansi_col, schema.as_ref()),
            "non-ANSI spark_modulo by column should be safe (guarded by null_if_zero)"
        );

        // 3. ANSI modulo by column is fallible (can raise DivideByZero) and must NOT be safe
        let ansi_col = create_modulo_expr(
            Arc::clone(&value),
            Arc::clone(&divisor),
            DataType::Int32,
            Arc::clone(&schema),
            true,
            &session.state(),
        )
        .unwrap();
        assert!(
            !is_safe_to_prune_before(&ansi_col, schema.as_ref()),
            "ANSI spark_modulo by column can error and must not be safe to prune before"
        );

        // 4. Modulo by zero or -1 in ANSI mode must not be safe
        for bad_divisor in [lit(0_i32), lit(-1_i32)] {
            let ansi_bad = create_modulo_expr(
                Arc::clone(&value),
                bad_divisor,
                DataType::Int32,
                Arc::clone(&schema),
                true,
                &session.state(),
            )
            .unwrap();
            assert!(
                !is_safe_to_prune_before(&ansi_bad, schema.as_ref()),
                "ANSI spark_modulo by 0 or -1 must not be safe"
            );
        }

        // 5. Non-deterministic expressions (rand) remain boundaries
        let random_pred: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(RandExpr::new(42)),
            Operator::Lt,
            lit(0.5_f64),
        ));
        assert!(
            !is_safe_to_prune_before(&random_pred, schema.as_ref()),
            "random predicate must not be safe"
        );
    }
}
