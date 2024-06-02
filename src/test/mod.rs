use crate::kvdb::KVDb;

pub mod latency_test;

pub trait Test {
    fn run(&self, db: &mut Box<dyn KVDb>);
}
