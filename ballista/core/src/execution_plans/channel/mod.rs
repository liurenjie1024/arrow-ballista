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

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use crate::{error::Result, serde::protobuf::ShuffleWritePartition};


/// An output channel receives one partiton of shuffle write data.
#[async_trait]
pub trait OutputChannel {
    async fn append(&mut self, record_batch: &RecordBatch) -> Result<()>;
    async fn finish(mut self) -> Result<ShuffleWritePartition>;
}