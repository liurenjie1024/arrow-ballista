pub mod service;
pub mod error;

pub mod spark {
    pub mod connect {
        include!(concat!("generated", "/spark.connect.rs"));
    }
}