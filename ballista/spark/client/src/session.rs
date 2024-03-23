use std::collections::HashMap;
use spark_connect_common::service::SparkSessionServiceRef;
use spark_connect_common::error::Result;
use crate::dataframe::DataFrame;

pub struct SparkSession {
    service: SparkSessionServiceRef,
}

impl SparkSession {
    pub fn table(&self, table_name: &str) -> Result<DataFrame> {
        todo!()
    }
}

pub struct SparkSessionBuilder {
    remote: bool,
    url: String,
    options: HashMap<String, String>,
}


impl SparkSessionBuilder {
    pub fn remote(url: &str) -> Self {
        Self {
            remote: true,
            url: url.to_owned(),
            options: HashMap::new(),
        }
    }

    pub fn local(master: &str) -> Self {
        Self {
            remote: false,
            url: master.to_owned(),
            options: HashMap::new(),
        }
    }

    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    pub async fn build(self) -> Result<SparkSession> {
        if self.remote {
            self.build_remote().await
        } else {
            self.build_local().await
        }
    }

    async fn build_remote(self) -> Result<SparkSession> {
        todo!()
    }

    async fn build_local(self) -> Result<SparkSession> {
        todo!()
    }
}

