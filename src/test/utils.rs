use std::{collections::HashSet, time::SystemTime};

use rand::Rng;

use super::Operation;

fn chance(num: u8) -> bool {
    rand::thread_rng().gen_range(0..10) < num
}

pub fn generate_random_operations(
    num_keys: u32,
    num_operations: u32,
    read_write_ratio: f32,
    set_delete_ratio: f32,
    hit_reads_ratio: f32,
) -> Vec<Operation> {
    let setup_start_time = SystemTime::now();
    let mut key_vector = vec![];
    for i in 1..=num_keys {
        key_vector.push(format!("key{}", i));
    }
    let mut operations = vec![];
    let mut num_reads = (read_write_ratio * num_operations as f32) as u32;
    let mut num_hit_reads = (hit_reads_ratio * num_reads as f32) as u32;
    let num_writes = num_operations - num_reads;
    let mut num_sets = (set_delete_ratio * num_writes as f32) as u32;
    let mut num_deletes = num_writes - num_sets;
    println!(
        "Generating {} read, {} set and {} delete operations",
        num_reads, num_sets, num_deletes
    );

    let mut used_keys = HashSet::new();
    let mut used_keys_vector: Vec<String> = Vec::new();
    for _ in 1..num_operations {
        let rand_key_index = rand::thread_rng().gen_range(0..num_keys) as usize;
        let key = key_vector[rand_key_index].clone();

        if (chance(7) && num_sets > 0)
            || (num_reads == 0 && num_deletes == 0)
            || used_keys_vector.is_empty()
        {
            if !used_keys.contains(&key) {
                used_keys_vector.push(key.clone());
                used_keys.insert(key.clone());
            }

            operations.push(Operation::Set(
                key,
                format!("{}", rand::thread_rng().gen_range(1..100000)),
            ));
            num_sets = num_sets - 1;
            continue;
        }

        let rand_key_index = rand::thread_rng().gen_range(0..used_keys_vector.len()) as usize;
        let known_key = used_keys_vector[rand_key_index].clone();
        if (chance(5) && num_deletes > 0) || num_reads == 0 {
            operations.push(Operation::Delete(known_key));
            num_deletes = num_deletes - 1;
            continue;
        }

        let mut key = key;
        if (chance(2) && num_hit_reads > 0) || num_hit_reads == num_reads {
            key = known_key;
            num_hit_reads -= 1;
        }

        operations.push(Operation::Read(key));
        num_reads = num_reads - 1;
        continue;
    }
    println!(
        "Finished generating random operations in {:?}",
        setup_start_time.elapsed().unwrap()
    );
    operations
}
