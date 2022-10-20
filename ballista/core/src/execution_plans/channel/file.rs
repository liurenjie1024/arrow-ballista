use std::path::{Path, PathBuf};

use async_trait::async_trait;
use datafusion::arrow::{datatypes::Schema, record_batch::RecordBatch};
use log::debug;

use crate::error::Result;
use crate::{
    execution_plans::shuffle_writer::ShuffleWriteMetrics,
    serde::protobuf::ShuffleWritePartition,
};

use super::OutputChannel;
use datafusion::physical_plan::common::IPCWriter;

pub(super) struct FileOutputChannel {
    partition_id: u64,
    path: String,
    writer: IPCWriter,
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
    pub(super) fn try_new<P: AsRef<Path>>(
        input_partition: u64,
        partitiond_id: u64,
        base_path: P,
        schema: &Schema,
        metrics: ShuffleWriteMetrics,
    ) -> Result<Self> {
        let mut path = base_path.as_ref().to_path_buf();
        path.push(&format!("{}", partitiond_id));
        std::fs::create_dir_all(&path)?;

        path.push(format!("data-{}.arrow", input_partition));
        debug!("Writing results to {:?}", path);

        Ok(Self {
            partition_id: partitiond_id,
            path: path.to_string_lossy().to_string(),
            writer: IPCWriter::new(path.as_ref(), schema)?,
            metrics,
        })
    }
}
