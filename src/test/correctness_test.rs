use std::collections::HashMap;

use super::{utils::generate_random_operations, Operation, Test};

pub struct CorrectnessTest {
    operations: Vec<Operation>,
}

impl Test for CorrectnessTest {
    fn run(&self, db: &mut Box<dyn crate::kvdb::KVDb>) {
        println!(
            "-------Running correctness test suite for {}-------",
            db.description()
        );
        let mut sot = HashMap::new();
        for op in &self.operations {
            match op {
                Operation::Read(ref key) => {
                    let want = sot.get(key);
                    match db.get(key) {
                        Ok(got) => {
                            if want != got.as_ref() {
                                println!(
                                    "Test failed: expected {:?} value for key {}, got {:?}",
                                    want, key, got
                                );
                                return;
                            }
                        }
                        Err(e) => println!("Test failed: unexpected error in read: {}", e),
                    }
                }
                Operation::Write(ref key, ref value) => {
                    sot.insert(key, value.clone());
                    if let Err(e) = db.set(key, value) {
                        println!("Test failed: unexpected error in write: {}", e);
                        return;
                    }
                }
                Operation::Delete(ref key) => {
                    sot.remove(key);
                    if let Err(e) = db.delete(key) {
                        println!("Test failed: unexpected error in delete: {}", e);
                        return;
                    }
                }
            }
        }
        println!("Test passed");
    }
}

impl CorrectnessTest {
    pub fn new(
        num_keys: u32,
        num_operations: u32,
        read_write_ratio: f32,
        set_delete_ratio: f32,
    ) -> CorrectnessTest {
        let operations = generate_random_operations(
            num_keys,
            num_operations,
            read_write_ratio,
            set_delete_ratio,
        );
        CorrectnessTest { operations }
    }
}
