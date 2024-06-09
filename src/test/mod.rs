use crate::kvdb::KVDb;

pub mod correctness_test;
pub mod latency_test;
mod utils;

pub trait Test {
    fn run(&self, db: &mut Box<dyn KVDb>);
}

enum Operation {
    Set(String, String),
    Delete(String),
    Read(String),
}
