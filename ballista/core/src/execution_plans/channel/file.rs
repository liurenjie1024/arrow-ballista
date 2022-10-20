use std::path::Path;

use async_trait::async_trait;
use datafusion::{arrow::{ipc::{writer::FileWriter, Schema}, record_batch::RecordBatch}, physical_plan::metrics};
use log::error;


use crate::{error::BallistaError, execution_plans::shuffle_writer::ShuffleWriteMetrics, serde::{scheduler::PartitionStats, protobuf::ShuffleWritePartition}};
use crate::error::Result;

use super::OutputChannel;

struct FileOutputChannel {
    partition_id: u64,
    path: String,
    writer: IpcWriter,
    metrics: ShuffleWriteMetrics,
}

#[async_trait]
impl OutputChannel for FileOutputChannel {
    async fn append(&mut self, record_batch: &RecordBatch) -> Result<()> {
        let timer = self.metrics.write_time.timer();
        self.writer.write(record_batch)?;
        timer.done();
        self.metrics.output_rows.add(record_batch.num_rows());
        Ok(())
    }

    async fn finish(mut self) -> Result<ShuffleWritePartition> {
        let timer = self.metrics.write_time.timer();
        self.writer.finish()?;
        timer.done();

        Ok(ShuffleWritePartition {
            partition_id: self.partition_id,
            path: self.path,
            num_batches: self.writer.num_batches,
            num_rows: self.writer.num_rows,
            num_bytes: self.writer.num_bytes,
        })
    }
}

impl FileOutputChannel {
    fn try_new<P: AsRef<Path>>(
        partitiond_id: u64,
        path: P,
        schema: &Schema,
        metrics: ShuffleWriteMetrics
        ) -> Result<Self> {

            Ok(Self {
                partition_id,
                path: path.as_ref().to_string_lossy().to_string(),
                writer: IpcWriter::new(path, schema),
                metrics
            })
    }
}