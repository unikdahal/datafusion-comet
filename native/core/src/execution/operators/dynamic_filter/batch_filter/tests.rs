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

use super::*;

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::cast::as_int32_array;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::prelude::SessionContext;

fn input(
    values: Vec<Option<i32>>,
    key_type: &DataType,
    key_index: usize,
) -> Arc<dyn ExecutionPlan> {
    let payload = Arc::new(Int32Array::from_iter_values(0..values.len() as i32)) as ArrayRef;
    let key = cast(&Int32Array::from(values), key_type).unwrap();
    let mut fields = vec![
        Field::new("key", key_type.clone(), true),
        Field::new("payload", DataType::Int32, false),
    ];
    let mut columns = vec![key, payload];
    fields.swap(0, key_index);
    columns.swap(0, key_index);
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    // Multiple build batches prove that an early subset of keys cannot prune
    // matches belonging to a later batch.
    let batches = if batch.num_rows() == 0 {
        vec![batch]
    } else {
        (0..batch.num_rows())
            .step_by(2)
            .map(|offset| batch.slice(offset, 2.min(batch.num_rows() - offset)))
            .collect()
    };
    memory_exec(batches)
}

fn memory_exec(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
    MemorySourceConfig::try_new_exec(std::slice::from_ref(&batches), batches[0].schema(), None)
        .unwrap()
}

fn metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        return metric(projection.input(), name);
    }
    plan.metrics()
        .unwrap()
        .sum_by_name(name)
        .unwrap()
        .as_usize()
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn probe_batch(values: Vec<Option<i32>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("payload", DataType::Int32, false),
        Field::new("key", DataType::Int32, true),
    ]));
    let payload = Arc::new(Int32Array::from_iter_values(0..values.len() as i32)) as ArrayRef;
    let key = Arc::new(Int32Array::from(values)) as ArrayRef;
    RecordBatch::try_new(schema, vec![payload, key]).unwrap()
}

fn key_less_than(bound: i32) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(
        Arc::new(Column::new("key", 1)),
        Operator::Lt,
        lit(bound),
    ))
}

fn filter_batches(
    batches: Vec<RecordBatch>,
) -> (Arc<dyn ExecutionPlan>, Arc<DynamicFilterPhysicalExpr>) {
    let predicate = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![Arc::new(Column::new("key", 1))],
        key_less_than(10),
    ));
    let wrapper = Arc::new(DynamicFilterExec::new(
        memory_exec(batches),
        Arc::clone(&predicate),
        ExecutionPlanMetricsSet::new(),
        "test_filter",
    ));
    (wrapper, predicate)
}

fn assert_accounting(plan: &Arc<dyn ExecutionPlan>, output_rows: usize, input_rows: usize) {
    let evaluated = metric(plan, "test_filter_rows_evaluated");
    let bypassed = metric(plan, "test_filter_rows_bypassed");
    let pruned = metric(plan, "test_filter_rows_pruned");
    assert_eq!(evaluated + bypassed, input_rows);
    assert_eq!(output_rows + pruned, evaluated + bypassed);
}

#[tokio::test]
async fn all_true_mask_preserves_column_allocations() {
    let batch = probe_batch(vec![Some(0); 128]);
    let (wrapper, _) = filter_batches(vec![batch.clone()]);
    let output = collect(Arc::clone(&wrapper), SessionContext::new().task_ctx())
        .await
        .unwrap();
    assert_eq!(output, vec![batch.clone()]);
    for (actual, original) in output[0].columns().iter().zip(batch.columns()) {
        assert!(Arc::ptr_eq(actual, original));
    }
    assert_eq!(metric(&wrapper, "test_filter_rows_pruned"), 0);
    assert_eq!(metric(&wrapper, "test_filter_rows_evaluated"), 128);
    assert_accounting(&wrapper, 128, 128);
}

#[tokio::test]
async fn empty_all_false_and_nullable_masks_preserve_accounting() {
    let batch = probe_batch(vec![Some(20); 128]);
    let (wrapper, _) = filter_batches(vec![
        batch.clone(),
        probe_batch(vec![]),
        probe_batch(vec![Some(0), None, Some(20), Some(1)]),
    ]);
    let output = collect(Arc::clone(&wrapper), SessionContext::new().task_ctx())
        .await
        .unwrap();
    assert_eq!(output[0], batch.slice(0, 0));
    let original = as_int32_array(batch.column(0).as_ref()).unwrap();
    let empty = as_int32_array(output[0].column(0).as_ref()).unwrap();
    assert_eq!(original.values().as_ptr(), empty.values().as_ptr());
    assert_eq!(output[1].num_rows(), 0);
    assert_eq!(output[2].num_rows(), 2);
    assert_eq!(metric(&wrapper, "test_filter_rows_pruned"), 130);
    assert_accounting(&wrapper, 2, 132);
}

#[tokio::test]
async fn non_selective_stream_bypasses_and_periodically_samples() {
    let rows = SELECTIVITY_WINDOW_ROWS / 8;
    let batch = probe_batch(vec![Some(0); rows]);
    let num_batches = 8 + 2 * BYPASS_SAMPLE_INTERVAL + 3;
    let (wrapper, _) = filter_batches(vec![batch; num_batches]);
    let output = collect(Arc::clone(&wrapper), SessionContext::new().task_ctx())
        .await
        .unwrap();
    assert_eq!(metric(&wrapper, "test_filter_rows_evaluated"), 10 * rows);
    assert_eq!(
        metric(&wrapper, "test_filter_rows_bypassed"),
        (num_batches - 10) * rows
    );
    assert_eq!(metric(&wrapper, "test_filter_rows_pruned"), 0);
    assert_eq!(metric(&wrapper, "test_filter_bypass_switches"), 1);
    assert_accounting(&wrapper, row_count(&output), num_batches * rows);
}

#[tokio::test]
async fn selective_suffix_reenables_filtering_without_a_predicate_update() {
    let rows = SELECTIVITY_WINDOW_ROWS / 8;
    let keep = probe_batch(vec![Some(0); rows]);
    let reject = probe_batch(vec![Some(20); rows]);
    let initial_batches = 8 + BYPASS_SAMPLE_INTERVAL - 1;
    let mut batches = vec![keep; initial_batches];
    batches.extend([reject.clone(), reject]);
    let (wrapper, _) = filter_batches(batches);
    let output = collect(Arc::clone(&wrapper), SessionContext::new().task_ctx())
        .await
        .unwrap();
    assert!(output[initial_batches..]
        .iter()
        .all(|batch| batch.num_rows() == 0));
    assert_eq!(metric(&wrapper, "test_filter_rows_pruned"), 2 * rows);
    assert_eq!(metric(&wrapper, "test_filter_rows_evaluated"), 10 * rows);
    assert_eq!(
        metric(&wrapper, "test_filter_rows_bypassed"),
        (BYPASS_SAMPLE_INTERVAL - 1) * rows
    );
    assert_eq!(metric(&wrapper, "test_filter_bypass_switches"), 1);
    assert_accounting(&wrapper, row_count(&output), (initial_batches + 2) * rows);
}

#[tokio::test]
async fn selective_streams_never_bypass_including_the_threshold() {
    // Exactly 10% must remain selective, not just the all-false fast path.
    for pruned_per_batch in [1000, 5000, 10_000] {
        let mut values = vec![Some(0); 10_000];
        values[..pruned_per_batch].fill(Some(20));
        let (wrapper, _) = filter_batches(vec![probe_batch(values); 24]);
        let output = collect(Arc::clone(&wrapper), SessionContext::new().task_ctx())
            .await
            .unwrap();
        assert_eq!(metric(&wrapper, "test_filter_rows_bypassed"), 0);
        assert_eq!(metric(&wrapper, "test_filter_bypass_switches"), 0);
        assert_eq!(
            metric(&wrapper, "test_filter_rows_pruned"),
            24 * pruned_per_batch
        );
        assert_accounting(&wrapper, row_count(&output), 240_000);
    }
}

#[tokio::test]
async fn producer_updates_reset_bypass_and_the_evaluation_window() {
    let rows = SELECTIVITY_WINDOW_ROWS / 8;
    let batch = probe_batch(vec![Some(0); rows]);
    let (wrapper, predicate) = filter_batches(vec![batch; 12]);
    let mut stream = wrapper
        .execute(0, SessionContext::new().task_ctx())
        .unwrap();
    for _ in 0..9 {
        assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), rows);
    }
    assert_eq!(metric(&wrapper, "test_filter_rows_bypassed"), rows);
    predicate.update(key_less_than(-1)).unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 0);
    // The new window contains only this producer generation's observations.
    predicate.update(key_less_than(10)).unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), rows);
    assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), rows);
    assert!(stream.next().await.is_none());
    assert_eq!(metric(&wrapper, "test_filter_rows_evaluated"), 11 * rows);
    assert_eq!(metric(&wrapper, "test_filter_rows_pruned"), rows);
    assert_eq!(metric(&wrapper, "test_filter_rows_bypassed"), rows);
    assert_eq!(metric(&wrapper, "test_filter_bypass_switches"), 1);
    assert_accounting(&wrapper, 11 * rows, 12 * rows);
}

#[tokio::test]
async fn placeholder_updates_and_errors_are_not_hidden() {
    let source = input((0..10).map(Some).collect(), &DataType::Int32, 1);
    let predicate = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![Arc::new(Column::new("key", 1))],
        lit(true),
    ));
    let wrapper: Arc<dyn ExecutionPlan> = Arc::new(DynamicFilterExec::new(
        source,
        Arc::clone(&predicate),
        ExecutionPlanMetricsSet::new(),
        "test_filter",
    ));
    let task = SessionContext::new().task_ctx();
    let mut stream = wrapper.execute(0, Arc::clone(&task)).unwrap();
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 2);
    predicate
        .update(Arc::new(BinaryExpr::new(
            Arc::new(Column::new("key", 1)),
            Operator::Lt,
            lit(2i32),
        )))
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 0);
    predicate.update(lit(false)).unwrap();
    while let Some(batch) = stream.next().await {
        assert_eq!(batch.unwrap().num_rows(), 0);
    }
    assert_eq!(metric(&wrapper, "test_filter_rows_bypassed"), 2);
    assert_eq!(metric(&wrapper, "test_filter_rows_pruned"), 8);
    assert_eq!(metric(&wrapper, "test_filter_rows_evaluated"), 8);

    // Reset must not preserve an old condition, even while another owner
    // still holds the previous predicate.
    let reset = Arc::clone(&wrapper).reset_state().unwrap();
    let reset_output = collect(Arc::clone(&reset), Arc::clone(&task))
        .await
        .unwrap();
    assert_eq!(row_count(&reset_output), 10);
    assert_eq!(metric(&reset, "test_filter_rows_pruned"), 0);
    assert_eq!(metric(&reset, "test_filter_rows_bypassed"), 10);

    predicate.update(lit(42i32)).unwrap();
    let error = collect(wrapper, task).await.unwrap_err();
    assert!(error.to_string().contains("must evaluate to a Boolean"));
}
