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
//! The matrix varies clause depth and payload width, with an extra clustered-row case to exercise
//! the contiguous-selection fast path. The primary interleaved workload forces non-contiguous
//! selections and makes repeated filter/concat costs visible while preserving first-match-wins.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use comet::execution::operators::{MergeInstructionExec, MergeRowsExec};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::{config::SessionConfig, TaskContext};
use datafusion::logical_expr::Operator as DFOperator;
use datafusion::physical_expr::expressions::{binary, col, lit};
use datafusion::physical_plan::{common::collect, ExecutionPlan};
use tokio::runtime::Runtime;

const ROWS_PER_BATCH: usize = 32_768;
const BATCHES: usize = 4;

#[derive(Clone, Copy)]
enum Distribution {
    Interleaved,
    Clustered,
}

impl Distribution {
    fn as_str(self) -> &'static str {
        match self {
            Self::Interleaved => "interleaved",
            Self::Clustered => "clustered",
        }
    }
}

#[derive(Clone, Copy)]
struct BenchCase {
    distribution: Distribution,
    payload_columns: usize,
    clauses: usize,
}

fn input_schema(payload_columns: usize) -> SchemaRef {
    let mut fields = vec![
        Field::new("bucket", DataType::Int32, false),
        Field::new("target_present", DataType::Boolean, false),
        Field::new("source_present", DataType::Boolean, false),
    ];
    for index in 0..payload_columns {
        fields.push(Field::new(format!("p{index}"), DataType::Int64, false));
    }
    Arc::new(Schema::new(fields))
}

fn output_schema(payload_columns: usize) -> SchemaRef {
    Arc::new(Schema::new(
        (0..payload_columns)
            .map(|index| Field::new(format!("p{index}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ))
}

fn input_batch(
    schema: &SchemaRef,
    batch_index: usize,
    payload_columns: usize,
    distribution: Distribution,
) -> RecordBatch {
    let bucket = match distribution {
        Distribution::Interleaved => Int32Array::from_iter_values(
            (0..ROWS_PER_BATCH).map(|row| (row % 100) as i32),
        ),
        Distribution::Clustered => Int32Array::from_iter_values(
            (0..ROWS_PER_BATCH).map(|row| ((row * 100) / ROWS_PER_BATCH) as i32),
        ),
    };

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(bucket),
        Arc::new(BooleanArray::from(vec![true; ROWS_PER_BATCH])),
        Arc::new(BooleanArray::from(vec![true; ROWS_PER_BATCH])),
    ];

    for column in 0..payload_columns {
        columns.push(Arc::new(Int64Array::from_iter_values(
            (0..ROWS_PER_BATCH).map(|row| {
                ((batch_index * ROWS_PER_BATCH + row) * payload_columns + column) as i64
            }),
        )));
    }

    RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
}

fn output_projection(
    schema: &SchemaRef,
    payload_columns: usize,
) -> Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
    (0..payload_columns)
        .map(|index| col(&format!("p{index}"), schema).unwrap())
        .collect()
}

fn matched_instructions(
    schema: &SchemaRef,
    clauses: usize,
    payload_columns: usize,
) -> Vec<MergeInstructionExec> {
    assert!(clauses >= 2);

    let mut instructions = Vec::with_capacity(clauses);
    for index in 1..clauses {
        let threshold = ((index * 100) / clauses) as i32;
        instructions.push(MergeInstructionExec {
            condition: binary(
                col("bucket", schema).unwrap(),
                DFOperator::Lt,
                lit(threshold),
                schema,
            )
            .unwrap(),
            outputs: vec![output_projection(schema, payload_columns)],
        });
    }
    instructions.push(MergeInstructionExec {
        condition: lit(true),
        outputs: vec![output_projection(schema, payload_columns)],
    });
    instructions
}

fn merge_plan(case: BenchCase) -> Arc<dyn ExecutionPlan> {
    let schema = input_schema(case.payload_columns);
    let batches = (0..BATCHES)
        .map(|batch_index| {
            input_batch(
                &schema,
                batch_index,
                case.payload_columns,
                case.distribution,
            )
        })
        .collect::<Vec<_>>();
    let source = MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None).unwrap();

    Arc::new(
        MergeRowsExec::try_new(
            col("source_present", &schema).unwrap(),
            col("target_present", &schema).unwrap(),
            matched_instructions(&schema, case.clauses, case.payload_columns),
            vec![],
            vec![],
            None,
            source,
            output_schema(case.payload_columns),
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
    // Keep each synthetic input batch intact. MemorySourceConfig otherwise splits batches at
    // the default session batch size, which turns the BooleanArray inputs into slices and
    // benchmarks an unrelated Arrow 59.3 sliced-and_not bug in the baseline implementation.
    let ctx = Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(ROWS_PER_BATCH)),
    );

    let mut cases = Vec::new();
    for payload_columns in [4usize, 12, 32] {
        for clauses in [2usize, 4, 8, 16] {
            cases.push(BenchCase {
                distribution: Distribution::Interleaved,
                payload_columns,
                clauses,
            });
        }
    }
    for clauses in [4usize, 8] {
        cases.push(BenchCase {
            distribution: Distribution::Clustered,
            payload_columns: 12,
            clauses,
        });
    }

    let mut group = c.benchmark_group("mergerows_clause_dispatch");
    for case in cases {
        let plan = merge_plan(case);
        let label = format!(
            "{}_w{}",
            case.distribution.as_str(),
            case.payload_columns
        );
        group.bench_with_input(
            BenchmarkId::new(label, case.clauses),
            &case,
            |b, _| b.iter(|| run(&runtime, &plan, &ctx)),
        );
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
