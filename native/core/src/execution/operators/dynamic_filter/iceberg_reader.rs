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

use std::sync::Arc;

use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, Literal,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_operators::CometFilterExec;
use iceberg::arrow::{RuntimePredicateProvider, RuntimePredicateSnapshot};
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;
use iceberg::{Error, ErrorKind, Result as IcebergResult};

use super::parquet_reader::is_direct_column_null_checks;
use crate::execution::operators::IcebergScanExec;

#[derive(Debug)]
struct IcebergRuntimePredicateProvider {
    predicate: Arc<DynamicFilterPhysicalExpr>,
    probe_column_index: usize,
    iceberg_field_name: String,
}

impl IcebergRuntimePredicateProvider {
    fn new(
        predicate: Arc<DynamicFilterPhysicalExpr>,
        probe_column_index: usize,
        iceberg_field_name: String,
    ) -> Self {
        Self {
            predicate,
            probe_column_index,
            iceberg_field_name,
        }
    }
}

impl RuntimePredicateProvider for IcebergRuntimePredicateProvider {
    fn snapshot(&self) -> IcebergResult<RuntimePredicateSnapshot> {
        let generation = self.predicate.snapshot_generation();
        let current = self.predicate.current().map_err(|error| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to snapshot runtime predicate: {error}"),
            )
        })?;
        Ok(RuntimePredicateSnapshot::new(
            extract_iceberg_predicate(&current, self.probe_column_index, &self.iceberg_field_name),
            generation,
        ))
    }
}

pub(super) fn try_attach_iceberg_reader_filter(
    input: &Arc<dyn ExecutionPlan>,
    predicate: Arc<DynamicFilterPhysicalExpr>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if input.fetch().is_some() {
        return Ok(None);
    }

    if let Some(filter) = input.downcast_ref::<CometFilterExec>() {
        if filter.has_projection() || !is_direct_column_null_checks(filter.predicate()) {
            return Ok(None);
        }
        let Some(reader) =
            try_attach_iceberg_reader_filter(filter.input(), Arc::clone(&predicate))?
        else {
            return Ok(None);
        };
        return match filter.with_execution_input(reader) {
            Ok(updated) => Ok(Some(updated)),
            Err(error) => {
                log::debug!("Iceberg runtime predicate attachment skipped: {error}");
                Ok(None)
            }
        };
    }

    let Some(scan) = input.downcast_ref::<IcebergScanExec>() else {
        return Ok(None);
    };
    let children = predicate.children();
    let [child] = children.as_slice() else {
        return Ok(None);
    };
    let Some(column) = child.downcast_ref::<Column>() else {
        return Ok(None);
    };

    let Some(iceberg_field_name) = scan.runtime_predicate_field_name(column.index()) else {
        return Ok(None);
    };
    let provider: Arc<dyn RuntimePredicateProvider> =
        Arc::new(IcebergRuntimePredicateProvider::new(
            Arc::clone(&predicate),
            column.index(),
            iceberg_field_name,
        ));
    Ok(Some(Arc::new(
        scan.with_runtime_predicate_provider(provider),
    )))
}

fn extract_iceberg_predicate(
    expr: &Arc<dyn PhysicalExpr>,
    probe_column_index: usize,
    iceberg_field_name: &str,
) -> Option<Predicate> {
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
        return extract_bound(binary, probe_column_index, iceberg_field_name);
    }

    expr.downcast_ref::<Literal>()
        .and_then(|literal| match literal.value() {
            ScalarValue::Boolean(Some(false)) => Some(Predicate::AlwaysFalse),
            _ => None,
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
        Operator::Eq => Some(reference.equal_to(datum)),
        Operator::Gt => Some(reference.greater_than(datum)),
        Operator::GtEq => Some(reference.greater_than_or_equal_to(datum)),
        Operator::Lt => Some(reference.less_than(datum)),
        Operator::LtEq => Some(reference.less_than_or_equal_to(datum)),
        _ => None,
    }
}

fn scalar_to_datum(value: &ScalarValue) -> Option<Datum> {
    match value {
        ScalarValue::Int8(Some(value)) => Some(Datum::int(i32::from(*value))),
        ScalarValue::Int16(Some(value)) => Some(Datum::int(i32::from(*value))),
        ScalarValue::Int32(Some(value)) => Some(Datum::int(*value)),
        ScalarValue::Int64(Some(value)) => Some(Datum::long(*value)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_expr::expressions::lit;

    #[test]
    fn extracts_signed_integer_range() {
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let lower: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&key),
            Operator::GtEq,
            lit(100_i32),
        ));
        let upper: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(key, Operator::LtEq, lit(199_i32)));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(lower, Operator::And, upper));

        let expected = Reference::new("iceberg_key")
            .greater_than_or_equal_to(Datum::int(100))
            .and(Reference::new("iceberg_key").less_than_or_equal_to(Datum::int(199)));
        assert_eq!(
            extract_iceberg_predicate(&expr, 0, "iceberg_key"),
            Some(expected)
        );
    }

    #[test]
    fn keeps_supported_bounds_with_unconvertible_conjunct() {
        let bound: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("key", 0)),
            Operator::GtEq,
            lit(10_i64),
        ));
        let expr: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(bound, Operator::And, lit(true)));

        assert_eq!(
            extract_iceberg_predicate(&expr, 0, "iceberg_key"),
            Some(Reference::new("iceberg_key").greater_than_or_equal_to(Datum::long(10)))
        );
    }

    #[test]
    fn handles_constant_filters() {
        let false_expr: Arc<dyn PhysicalExpr> = lit(false);
        let true_expr: Arc<dyn PhysicalExpr> = lit(true);
        assert_eq!(
            extract_iceberg_predicate(&false_expr, 0, "iceberg_key"),
            Some(Predicate::AlwaysFalse)
        );
        assert_eq!(
            extract_iceberg_predicate(&true_expr, 0, "iceberg_key"),
            None
        );
    }
}
