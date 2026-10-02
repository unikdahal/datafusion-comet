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

//! Micro-benchmark for MergeRows clause dispatch.
//!
//! The workload is intentionally wide and uses ordered clauses that each claim a fraction of the
//! remaining matched rows. This makes the cost of repeatedly filtering the working RecordBatch and
//! concatenating action outputs visible while still exercising first-match-wins semantics.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use comet::execution::operators::{MergeInstructionExec, MergeRowsExec};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator as DFOperator;
use datafusion::physical_expr::expressions::{binary, col, lit};
use datafusion::physical_plan::{common::collect, ExecutionPlan};
use tokio::runtime::Runtime;

const ROWS_PER_BATCH: usize = 32_768;
const BATCHES: usize = 4;
const PAYLOAD_COLUMNS: usize = 12;

fn input_schema() -> SchemaRef {
    let mut fields = vec![
        Field::new("bucket", DataType::Int32, false),
        Field::new("target_present", DataType::Boolean, false),
        Field::new("source_present", DataType::Boolean, false),
    ];
    for index in 0..PAYLOAD_COLUMNS {
        fields.push(Field::new(format!("p{index}"), DataType::Int64, false));
    }
    Arc::new(Schema::new(fields))
}

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(
        (0..PAYLOAD_COLUMNS)
            .map(|index| Field::new(format!("p{index}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ))
}

fn input_batch(schema: &SchemaRef, batch_index: usize) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from_iter_values(
            (0..ROWS_PER_BATCH).map(|row| (row % 100) as i32),
        )),
        Arc::new(BooleanArray::from(vec![true; ROWS_PER_BATCH])),
        Arc::new(BooleanArray::from(vec![true; ROWS_PER_BATCH])),
    ];

    for column in 0..PAYLOAD_COLUMNS {
        columns.push(Arc::new(Int64Array::from_iter_values(
            (0..ROWS_PER_BATCH)
                .map(|row| ((batch_index * ROWS_PER_BATCH + row) * PAYLOAD_COLUMNS + column) as i64),
        )));
    }

    RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
}

fn output_projection(schema: &SchemaRef) -> Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
    (0..PAYLOAD_COLUMNS)
        .map(|index| col(&format!("p{index}"), schema).unwrap())
        .collect()
}

fn matched_instructions(
    schema: &SchemaRef,
    clauses: usize,
) -> Vec<MergeInstructionExec> {
    let thresholds: &[i32] = match clauses {
        4 => &[25, 50, 75],
        8 => &[12, 25, 37, 50, 62, 75, 87],
        _ => panic!("unsupported clause count"),
    };

    let mut instructions = Vec::with_capacity(clauses);
    for threshold in thresholds {
        instructions.push(MergeInstructionExec {
            condition: binary(
                col("bucket", schema).unwrap(),
                DFOperator::Lt,
                lit(*threshold),
                schema,
            )
            .unwrap(),
            outputs: vec![output_projection(schema)],
        });
    }
    instructions.push(MergeInstructionExec {
        condition: lit(true),
        outputs: vec![output_projection(schema)],
    });
    instructions
}

fn merge_plan(clauses: usize) -> Arc<dyn ExecutionPlan> {
    let schema = input_schema();
    let batches = (0..BATCHES)
        .map(|batch_index| input_batch(&schema, batch_index))
        .collect::<Vec<_>>();
    let source = MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None).unwrap();

    Arc::new(
        MergeRowsExec::try_new(
            col("source_present", &schema).unwrap(),
            col("target_present", &schema).unwrap(),
            matched_instructions(&schema, clauses),
            vec![],
            vec![],
            None,
            source,
            output_schema(),
        )
        .unwrap(),
    )
}

fn run(runtime: &Runtime, plan: &Arc<dyn ExecutionPlan>, ctx: &Arc<TaskContext>) {
    let stream = plan.execute(0, Arc::clone(ctx)).unwrap();
    let batches = runtime.block_on(collect(stream)).unwrap();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, ROWS_PER_BATCH * BATCHES);
    std::hint::black_box(batches);
}

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let ctx = Arc::new(TaskContext::default());

    let mut group = c.benchmark_group("mergerows_clause_dispatch");
    for clauses in [4usize, 8] {
        let plan = merge_plan(clauses);
        group.bench_with_input(BenchmarkId::from_parameter(clauses), &clauses, |b, _| {
            b.iter(|| run(&runtime, &plan, &ctx))
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
