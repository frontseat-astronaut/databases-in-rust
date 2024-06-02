use std::time::{Duration, SystemTime};

use rand::Rng;

use super::Test;

enum Operation {
    Read(String),
    Write(String, String),
}

pub struct LatencyTest {
    operations: Vec<Operation>,
}

impl Test for LatencyTest {
    fn run(&self, db: &mut Box<dyn crate::kvdb::KVDb>) {
        let mut read_times = Duration::new(0, 0);
        let mut num_reads = 0;
        let mut write_times = Duration::new(0, 0);
        let mut num_writes = 0;
        let db_ops_start_time = SystemTime::now();
        for op in &self.operations {
            match op {
                Operation::Read(key) => {
                    let read_start_time = SystemTime::now();
                    db.get(&key).unwrap();
                    read_times += read_start_time.elapsed().unwrap();
                    num_reads += 1;
                }
                Operation::Write(key, value) => {
                    let write_start_time = SystemTime::now();
                    db.set(&key, &value).unwrap();
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
    pub fn new(num_keys: u32, num_operations: u32, read_write_ratio: f32) -> LatencyTest {
        println!(
            "Creating latency test suite with {} operations on {} keys, and a Read/Write ratio of {}",
            num_operations, num_keys, read_write_ratio
        );
        let setup_start_time = SystemTime::now();
        let mut key_vector = vec![];
        for i in 1..=num_keys {
            key_vector.push(format!("key{}", i));
        }
        let mut operations = vec![];
        let mut num_reads = (read_write_ratio * num_operations as f32) as u32;
        let mut num_writes = num_operations - num_reads;
        for _ in 1..num_operations {
            let rand_key_index = rand::thread_rng().gen_range(0..num_keys) as usize;
            let random_choice = rand::thread_rng().gen_range(0..2);
            if (random_choice == 0 && num_reads > 0) || (num_writes == 0) {
                operations.push(Operation::Read(key_vector[rand_key_index].clone()));
                num_reads = num_reads - 1;
            } else {
                operations.push(Operation::Write(
                    key_vector[rand_key_index].clone(),
                    format!("{}", rand::thread_rng().gen_range(1..100000)),
                ));
                num_writes = num_writes - 1;
            }
        }
        println!(
            "Finished test suite setup in {:?}",
            setup_start_time.elapsed().unwrap()
        );

        LatencyTest { operations }
    }
}
