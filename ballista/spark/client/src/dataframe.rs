use arrow::array::RecordBatch;
use spark_connect_common::service::SparkSessionServiceRef;
use spark_connect_common::spark::connect::Plan;

use spark_connect_common::error::Result;

pub struct DataFrame {
    service: SparkSessionServiceRef,
    plan: Plan,
}

impl DataFrame {
    pub fn new(service: SparkSessionServiceRef, plan: Plan) -> Self {
        Self { service, plan }
    }

    pub async fn collect(&self) -> Result<RecordBatch> {
        self.service.execute(self.plan.clone()).await
    }
} 