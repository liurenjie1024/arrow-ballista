use std::collections::HashMap;
use std::sync::Arc;
use crate::session::SparkServerSessionRef;
use tokio::sync::Mutex;

pub struct SparkSessionManager {
    sessions: Arc<Mutex<HashMap<String, SparkServerSessionRef>>>,
}