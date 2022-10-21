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

//! ShuffleWriterExec represents a section of a query plan that has consistent partitioning and
//! can be executed as one unit with each partition being executed in parallel. The output of each
//! partition is re-partitioned and streamed to disk in Arrow IPC format. Future stages of the query
//! will use the ShuffleReaderExec to read these results.

use datafusion::physical_plan::expressions::PhysicalSortExpr;


use std::any::Any;
use std::future::Future;
use std::iter::Iterator;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use crate::execution_plans::channel::new_file_channel;


use crate::serde::protobuf::ShuffleWritePartition;
use crate::serde::scheduler::PartitionStats;
use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, StringBuilder, StructBuilder, UInt32Builder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};

use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{
    self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};

use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use futures::{StreamExt, TryFutureExt, TryStreamExt};

use datafusion::arrow::error::ArrowError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use log::{debug};

use super::channel::write_stream_to_channels;

/// ShuffleWriterExec represents a section of a query plan that has consistent partitioning and
/// can be executed as one unit with each partition being executed in parallel. The output of each
/// partition is re-partitioned and streamed to disk in Arrow IPC format. Future stages of the query
/// will use the ShuffleReaderExec to read these results.
#[derive(Debug, Clone)]
pub struct ShuffleWriterExec {
    /// Unique ID for the job (query) that this stage is a part of
    job_id: String,
    /// Unique query stage ID within the job
    stage_id: usize,
    /// Physical execution plan for this query stage
    plan: Arc<dyn ExecutionPlan>,
    /// Path to write output streams to
    work_dir: String,
    /// Optional shuffle output partitioning
    shuffle_output_partitioning: Option<Partitioning>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

#[derive(Debug, Clone)]
pub(in crate::execution_plans) struct ShuffleWriteMetrics {
    /// Time spend writing batches to shuffle files
    pub(in crate::execution_plans) write_time: metrics::Time,
    pub(in crate::execution_plans) repart_time: metrics::Time,
    pub(in crate::execution_plans) input_rows: metrics::Count,
    pub(in crate::execution_plans) output_rows: metrics::Count,
}

impl ShuffleWriteMetrics {
    fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let write_time = MetricBuilder::new(metrics).subset_time("write_time", partition);
        let repart_time =
            MetricBuilder::new(metrics).subset_time("repart_time", partition);

        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);

        let output_rows = MetricBuilder::new(metrics).output_rows(partition);

        Self {
            write_time,
            repart_time,
            input_rows,
            output_rows,
        }
    }
}

impl ShuffleWriterExec {
    /// Create a new shuffle writer
    pub fn try_new(
        job_id: String,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: String,
        shuffle_output_partitioning: Option<Partitioning>,
    ) -> Result<Self> {
        Ok(Self {
            job_id,
            stage_id,
            plan,
            work_dir,
            shuffle_output_partitioning,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Get the Job ID for this query stage
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Get the Stage ID for this query stage
    pub fn stage_id(&self) -> usize {
        self.stage_id
    }

    /// Get the true output partitioning
    pub fn shuffle_output_partitioning(&self) -> Option<&Partitioning> {
        self.shuffle_output_partitioning.as_ref()
    }

    pub fn execute_shuffle_write(
        &self,
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> impl Future<Output = Result<Vec<ShuffleWritePartition>>> {
        let mut path = PathBuf::from(&self.work_dir);
        path.push(&self.job_id);
        path.push(&format!("{}", self.stage_id));

        let write_metrics = ShuffleWriteMetrics::new(input_partition, &self.metrics);
        let output_partitioning = self.shuffle_output_partitioning.clone();
        let plan = self.plan.clone();

        async move {
            let _now = Instant::now();
            let mut stream = plan.execute(input_partition, context)?;
            let partitioner = output_partitioning
                .map(|p| match p {
                    Partitioning::Hash(exprs, num_output_partitions) => Ok((
                        BatchPartitioner::try_new(
                            Partitioning::Hash(exprs, num_output_partitions),
                            write_metrics.repart_time.clone(),
                        )?,
                        num_output_partitions,
                    )),
                    _ => Err(DataFusionError::Execution(
                        "Invalid shuffle partitioning scheme".to_owned(),
                    )),
                })
                .transpose()?;

            let schema = stream.schema();
            let write_metrics_cloned = write_metrics.clone();
            write_stream_to_channels(
                &mut stream,
                move |output_partition| {
                    let base_path = path.clone();
                    new_file_channel(
                        input_partition as u64,
                        output_partition,
                        base_path,
                        schema.as_ref(),
                        write_metrics.clone(),
                    )
                },
                partitioner,
                write_metrics_cloned,
            )
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))
        }
    }
}

impl ExecutionPlan for ShuffleWriterExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        // This operator needs to be executed once for each *input* partition and there
        // isn't really a mechanism yet in DataFusion to support this use case so we report
        // the input partitioning as the output partitioning here. The executor reports
        // output partition meta data back to the scheduler.
        self.plan.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.plan.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ShuffleWriterExec::try_new(
            self.job_id.clone(),
            self.stage_id,
            children[0].clone(),
            self.work_dir.clone(),
            self.shuffle_output_partitioning.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = result_schema();

        let schema_captured = schema.clone();
        let fut_stream = self
            .execute_shuffle_write(partition, context)
            .and_then(|part_loc| async move {
                // build metadata result batch
                let num_writers = part_loc.len();
                let mut partition_builder = UInt32Builder::with_capacity(num_writers);
                let mut path_builder =
                    StringBuilder::with_capacity(num_writers, num_writers * 100);
                let mut num_rows_builder = UInt64Builder::with_capacity(num_writers);
                let mut num_batches_builder = UInt64Builder::with_capacity(num_writers);
                let mut num_bytes_builder = UInt64Builder::with_capacity(num_writers);

                for loc in &part_loc {
                    path_builder.append_value(loc.path.clone());
                    partition_builder.append_value(loc.partition_id as u32);
                    num_rows_builder.append_value(loc.num_rows);
                    num_batches_builder.append_value(loc.num_batches);
                    num_bytes_builder.append_value(loc.num_bytes);
                }

                // build arrays
                let partition_num: ArrayRef = Arc::new(partition_builder.finish());
                let path: ArrayRef = Arc::new(path_builder.finish());
                let field_builders: Vec<Box<dyn ArrayBuilder>> = vec![
                    Box::new(num_rows_builder),
                    Box::new(num_batches_builder),
                    Box::new(num_bytes_builder),
                ];
                let mut stats_builder = StructBuilder::new(
                    PartitionStats::default().arrow_struct_fields(),
                    field_builders,
                );
                for _ in 0..num_writers {
                    stats_builder.append(true);
                }
                let stats = Arc::new(stats_builder.finish());

                // build result batch containing metadata
                let batch = RecordBatch::try_new(
                    schema_captured.clone(),
                    vec![partition_num, path, stats],
                )?;

                debug!("RESULTS METADATA:\n{:?}", batch);

                MemoryStream::try_new(vec![batch], schema_captured, None)
            })
            .map_err(|e| ArrowError::ExternalError(Box::new(e)));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(fut_stream).try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "ShuffleWriterExec: {:?}",
                    self.shuffle_output_partitioning
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.plan.statistics()
    }
}

fn result_schema() -> SchemaRef {
    let stats = PartitionStats::default();
    Arc::new(Schema::new(vec![
        Field::new("partition", DataType::UInt32, false),
        Field::new("path", DataType::Utf8, false),
        stats.arrow_struct_repr(),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{StringArray, StructArray, UInt32Array, UInt64Array};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::expressions::Column;

    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::prelude::SessionContext;
    use tempfile::TempDir;

    #[tokio::test]
    // number of rows in each partition is a function of the hash output, so don't test here
    #[cfg(not(feature = "force_hash_collisions"))]
    async fn test() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let input_plan = Arc::new(CoalescePartitionsExec::new(create_input_plan()?));
        let work_dir = TempDir::new()?;
        let query_stage = ShuffleWriterExec::try_new(
            "jobOne".to_owned(),
            1,
            input_plan,
            work_dir.into_path().to_str().unwrap().to_owned(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2)),
        )?;
        let mut stream = query_stage.execute(0, task_ctx)?;
        let batches = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
        assert_eq!(1, batches.len());
        let batch = &batches[0];
        assert_eq!(3, batch.num_columns());
        assert_eq!(2, batch.num_rows());
        let path = batch.columns()[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let file0 = path.value(0);
        assert!(
            file0.ends_with("/jobOne/1/0/data-0.arrow")
                || file0.ends_with("\\jobOne\\1\\0\\data-0.arrow")
        );
        let file1 = path.value(1);
        assert!(
            file1.ends_with("/jobOne/1/1/data-0.arrow")
                || file1.ends_with("\\jobOne\\1\\1\\data-0.arrow")
        );

        let stats = batch.columns()[2]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let num_rows = stats
            .column_by_name("num_rows")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(4, num_rows.value(0));
        assert_eq!(4, num_rows.value(1));

        Ok(())
    }

    #[tokio::test]
    // number of rows in each partition is a function of the hash output, so don't test here
    #[cfg(not(feature = "force_hash_collisions"))]
    async fn test_partitioned() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let input_plan = create_input_plan()?;
        let work_dir = TempDir::new()?;
        let query_stage = ShuffleWriterExec::try_new(
            "jobOne".to_owned(),
            1,
            input_plan,
            work_dir.into_path().to_str().unwrap().to_owned(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2)),
        )?;
        let mut stream = query_stage.execute(0, task_ctx)?;
        let batches = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
        assert_eq!(1, batches.len());
        let batch = &batches[0];
        assert_eq!(3, batch.num_columns());
        assert_eq!(2, batch.num_rows());
        let stats = batch.columns()[2]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let num_rows = stats
            .column_by_name("num_rows")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(2, num_rows.value(0));
        assert_eq!(2, num_rows.value(1));

        Ok(())
    }

    fn create_input_plan() -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
            ],
        )?;
        let partition = vec![batch.clone(), batch];
        let partitions = vec![partition.clone(), partition];
        Ok(Arc::new(MemoryExec::try_new(&partitions, schema, None)?))
    }
}
