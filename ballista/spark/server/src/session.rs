use std::sync::Arc;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use spark_connect_common::service::SparkSessionService;
use spark_connect_common::spark::connect::Plan;

pub struct SparkServerSession {}

pub type SparkServerSessionRef = Arc<SparkServerSession>;

#[async_trait]
impl SparkSessionService for SparkServerSession {
    async fn execute(&self, plan: Plan) -> spark_connect_common::error::Result<RecordBatch> {

    }
}