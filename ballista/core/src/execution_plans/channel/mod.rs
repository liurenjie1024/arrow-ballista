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

//! This module contains input/output channels for shffule.

mod file;

use core::num;
use std::{path::Path, pin::Pin, vec};

use crate::{error::Result, serde::protobuf::ShuffleWritePartition};
use async_trait::async_trait;
use datafusion::{
    arrow::{datatypes::Schema, record_batch::RecordBatch},
    physical_plan::{repartition::BatchPartitioner, RecordBatchStream},
};

use self::file::FileOutputChannel;

use super::shuffle_writer::ShuffleWriteMetrics;

/// An output channel receives one partiton of shuffle write data.
#[async_trait]
pub trait OutputChannel {
    async fn append(&mut self, record_batch: &RecordBatch) -> Result<()>;
    async fn finish(mut self) -> Result<ShuffleWritePartition>;
}

pub type OutputChannelFactor<C: OutputChannel> = FnMut(u64) -> Result<C>;

pub(in crate::execution_plans) fn new_file_channel<P: AsRef<Path>>(
    input_partition: u64,
    output_partition_id: u64,
    base_path: P,
    schema: &Schema,
    metrics: ShuffleWriteMetrics,
) -> Result<impl OutputChannel> {
    FileOutputChannel::try_new(
        input_partition,
        output_partition_id,
        base_path,
        schema,
        metrics,
    )
}

pub(in crate::execution_plans) async fn write_stream_to_channels<F, C>(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
    channel_factory: F,
    partition: Option<(BatchPartitioner, usize)>,
    write_metrics: ShuffleWriteMetrics,
) -> Result<Vec<ShuffleWritePartition>>
where
    C: OutputChannel,
    F: FnMut(u64) -> Result<C>,
{
    let num_output_partitions = partition.map(|p| p.2).unwrap_or(1usize);
    let mut output_channels: Vec<Option<C>> = vec![None; num_output_partitions];

    while let Some(result) = stream.next().await {
        let input_batch = result?;

        write_metrics.input_rows.add(input_batch.num_rows());

        match partition {
            Some((partitioner, _)) => {
                partitioner.partition(
                    input_batch,
                    |output_partition, output_batch| {

                        let output_channel = output_channels[]
                        match &mut writers[output_partition] {
                            Some(w) => {
                                w.write(&output_batch)?;
                            }
                            None => {
                                let mut path = path.clone();
                                path.push(&format!("{}", output_partition));
                                std::fs::create_dir_all(&path)?;

                                path.push(format!("data-{}.arrow", input_partition));
                                debug!("Writing results to {:?}", path);

                                let mut writer =
                                    IPCWriter::new(&path, stream.schema().as_ref())?;

                                writer.write(&output_batch)?;
                                writers[output_partition] = Some(writer);
                            }
                        }
                        write_metrics.output_rows.add(output_batch.num_rows());
                        timer.done();
                        Ok(())
                    },
                )?;
            }
        }
    }
}
