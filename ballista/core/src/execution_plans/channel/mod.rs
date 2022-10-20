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

use std::path::Path;

use crate::{error::Result, serde::protobuf::ShuffleWritePartition};
use async_trait::async_trait;
use datafusion::arrow::{datatypes::Schema, record_batch::RecordBatch};

use self::file::FileOutputChannel;

use super::shuffle_writer::ShuffleWriteMetrics;

/// An output channel receives one partiton of shuffle write data.
#[async_trait]
pub trait OutputChannel {
    async fn append(&mut self, record_batch: &RecordBatch) -> Result<()>;
    async fn finish(mut self) -> Result<ShuffleWritePartition>;
}

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

pub(in crate::execution_plans) async fn write_stream_to_channel(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
    path: &str,
    disk_write_metric: &metrics::Time,
) -> Result<PartitionStats> {
}
