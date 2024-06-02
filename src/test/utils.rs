use std::time::SystemTime;

use rand::Rng;

use super::Operation;

pub fn generate_random_operations(
    num_keys: u32,
    num_operations: u32,
    read_write_ratio: f32,
    set_delete_ratio: f32,
) -> Vec<Operation> {
    let setup_start_time = SystemTime::now();
    let mut key_vector = vec![];
    for i in 1..=num_keys {
        key_vector.push(format!("key{}", i));
    }
    let mut operations = vec![];
    let mut num_reads = (read_write_ratio * num_operations as f32) as u32;
    let num_writes = num_operations - num_reads;
    let mut num_sets = (set_delete_ratio * num_writes as f32) as u32;
    let mut num_deletes = num_writes - num_sets;
    println!(
        "Generating {} read, {} set and {} delete operations",
        num_reads, num_sets, num_deletes
    );
    for _ in 1..num_operations {
        let rand_key_index = rand::thread_rng().gen_range(0..num_keys) as usize;
        let random_choice = rand::thread_rng().gen_range(0..3);
        if (random_choice == 0 && num_reads > 0) || (num_sets == 0 && num_deletes == 0) {
            operations.push(Operation::Read(key_vector[rand_key_index].clone()));
            num_reads = num_reads - 1;
        } else if (random_choice == 1 && num_sets > 0) || num_deletes == 0 {
            operations.push(Operation::Write(
                key_vector[rand_key_index].clone(),
                format!("{}", rand::thread_rng().gen_range(1..100000)),
            ));
            num_sets = num_sets - 1;
        } else {
            operations.push(Operation::Delete(key_vector[rand_key_index].clone()));
            num_deletes = num_deletes - 1;
        }
    }
    println!(
        "Finished generating random operations in {:?}",
        setup_start_time.elapsed().unwrap()
    );
    operations
}
