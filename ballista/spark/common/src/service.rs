use std::sync::Arc;
use arrow::array::RecordBatch;
use crate::spark::connect::Plan;
use crate::error::Result;

#[async_trait::async_trait]
pub trait SparkSessionService {
    async fn execute(&self, plan: Plan) -> Result<RecordBatch>;
}

pub type SparkSessionServiceRef = Arc<dyn SparkSessionService>;