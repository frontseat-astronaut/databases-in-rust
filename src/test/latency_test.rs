use std::time::{Duration, SystemTime};

use crate::test::utils::generate_random_operations;

use super::{Operation, Test};

pub struct LatencyTest {
    operations: Vec<Operation>,
}

impl Test for LatencyTest {
    fn run(&self, db: &mut Box<dyn crate::kvdb::KVDb>) {
        println!(
            "-------Running latency test suite for {}-------",
            db.description()
        );
        let mut read_times = Duration::new(0, 0);
        let mut num_reads = 0;
        let mut write_times = Duration::new(0, 0);
        let mut num_writes = 0;
        let db_ops_start_time = SystemTime::now();
        for op in &self.operations {
            match op {
                Operation::Read(ref key) => {
                    let read_start_time = SystemTime::now();
                    db.get(key).unwrap();
                    read_times += read_start_time.elapsed().unwrap();
                    num_reads += 1;
                }
                Operation::Set(ref key, ref value) => {
                    let write_start_time = SystemTime::now();
                    db.set(key, value).unwrap();
                    write_times += write_start_time.elapsed().unwrap();
                    num_writes += 1;
                }
                Operation::Delete(ref key) => {
                    let write_start_time = SystemTime::now();
                    db.delete(key).unwrap();
                    write_times += write_start_time.elapsed().unwrap();
                    num_writes += 1;
                }
            }
        }
        println!(
            "Finished latency test in {:?}",
            db_ops_start_time.elapsed().unwrap()
        );
        println!("Average latencies per operation:");
        println!("  Read: {:?}", read_times / num_reads,);
        println!("  Write: {:?}", write_times / num_writes,);
    }
}

impl LatencyTest {
    pub fn new(
        num_keys: u32,
        num_operations: u32,
        read_write_ratio: f32,
        set_delete_ratio: f32,
        hit_reads_ratio: f32,
    ) -> LatencyTest {
        let operations = generate_random_operations(
            num_keys,
            num_operations,
            read_write_ratio,
            set_delete_ratio,
            hit_reads_ratio,
        );
        LatencyTest { operations }
    }
}
