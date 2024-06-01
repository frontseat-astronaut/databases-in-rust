use super::KVDb;
use rand::Rng;
use std::time::{Duration, SystemTime};

pub struct Test {
    db: Box<dyn KVDb>,
}

impl Test {
    pub fn new(db: Box<dyn KVDb>) -> Test {
        Test { db }
    }

    pub fn test_updates(&mut self, num_keys: u32, num_operations: u32, read_write_ratio: f32) {
        enum Operation {
            Read(String),
            Write(String, String),
        }

        println!(
            "Testing {} operations on {} keys, with a Read/Write ratio of {}",
            num_operations, num_keys, read_write_ratio
        );
        let setup_start_time = SystemTime::now();
        let mut key_vector = vec![];
        for i in 1..=num_keys {
            key_vector.push(format!("key{}", i));
        }
        let mut operation_vector = vec![];
        let mut num_reads = (read_write_ratio * num_operations as f32) as u32;
        let mut num_writes = num_operations - num_reads;
        for _ in 1..num_operations {
            let rand_key_index = rand::thread_rng().gen_range(0..num_keys) as usize;
            let random_choice = rand::thread_rng().gen_range(0..2);
            if (random_choice == 0 && num_reads > 0) || (num_writes == 0) {
                operation_vector.push(Operation::Read(key_vector[rand_key_index].clone()));
                num_reads = num_reads - 1;
            } else {
                operation_vector.push(Operation::Write(
                    key_vector[rand_key_index].clone(),
                    Self::create_random_value(),
                ));
                num_writes = num_writes - 1;
            }
        }
        print!(
            "It took {:?} for setup. ",
            setup_start_time.elapsed().unwrap()
        );

        let mut read_times = Duration::new(0, 0);
        let mut num_reads = 0;
        let mut write_times = Duration::new(0, 0);
        let mut num_writes = 0;
        for op in operation_vector {
            match op {
                Operation::Read(key) => {
                    let read_start_time = SystemTime::now();
                    self.db.get(&key).unwrap();
                    read_times += read_start_time.elapsed().unwrap();
                    num_reads += 1;
                }
                Operation::Write(key, value) => {
                    let write_start_time = SystemTime::now();
                    self.db.set(&key, &value).unwrap();
                    write_times += write_start_time.elapsed().unwrap();
                    num_writes += 1;
                }
            }
        }
        println!(
            "It took {:?} for reads and {:?} for writes",
            read_times / num_reads,
            write_times / num_writes
        );
    }
    pub fn run(&mut self) {
        println!("starting test");
        self.get_value_for_test("k1");
        self.get_value_for_test("k2");
        self.get_value_for_test("k3");
        self.get_value_for_test("k4");

        /* these cases throw errors */
        // self.set_key_value_for_test("k,", "v");
        // self.get_value_for_test("k,");
        // self.set_key_value_for_test("k", "🪦");
        // self.get_value_for_test("k");

        self.set_key_value_for_test("k1", "v11");
        self.get_value_for_test("k1");
        self.set_key_value_for_test("k1", "v12");
        self.get_value_for_test("k1");

        self.set_random_value_for_test("k4");
        self.get_value_for_test("k4");

        self.set_random_value_for_test("k3");
        self.get_value_for_test("k3");

        self.set_key_value_for_test("k2", "v21");
        self.get_value_for_test("k2");
        self.delete_key_value_for_test("k2");
        self.get_value_for_test("k2");

        println!("");
    }

    fn set_random_value_for_test(&mut self, key: &str) {
        self.set_key_value_for_test(key, &Self::create_random_value())
    }
    fn set_key_value_for_test(&mut self, key: &str, value: &str) {
        match self.db.set(key, value) {
            Ok(()) => {
                println!("set value {} for key {} successfully", value, key)
            }
            Err(e) => {
                println!("error setting value for key {}: {}", key, e)
            }
        }
    }
    fn delete_key_value_for_test(&mut self, key: &str) {
        match self.db.delete(key) {
            Ok(()) => {
                println!("deleted key {} successfully", key)
            }
            Err(e) => {
                println!("error deleting key {}: {}", key, e)
            }
        }
    }
    fn get_value_for_test(&mut self, key: &str) {
        match self.db.get(key) {
            Ok(Some(value)) => {
                println!("key {} has value {}", key, value)
            }
            Ok(None) => {
                println!("key {} does not exist", key)
            }
            Err(e) => {
                println!("error getting value for key {}: {}", key, e)
            }
        }
    }

    fn create_random_value() -> String {
        format!("{}", rand::thread_rng().gen_range(1..100000))
    }
}
