use std::{
    collections::HashSet,
    fs::{create_dir_all, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    time::SystemTime,
};

use rand::Rng;

use super::Operation;

fn chance(num: u8) -> bool {
    rand::thread_rng().gen_range(0..10) < num
}

const DIR_PATH: &str = "./test_cases/";

pub fn generate_random_operations(
    num_keys: u32,
    num_operations: u32,
    read_write_ratio: f32,
    set_delete_ratio: f32,
    hit_reads_ratio: f32,
    save: bool,
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
        if num_hit_reads > 0 {
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

    if save {
        create_dir_all(DIR_PATH).unwrap();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(format!(
                "{DIR_PATH}{num_keys}_{num_operations}_{read_write_ratio}_{set_delete_ratio}_{hit_reads_ratio}"
            )).unwrap();
        for op in operations.iter() {
            match op {
                Operation::Set(key, value) => writeln!(&mut file, "S {key} {value}").unwrap(),
                Operation::Delete(key) => writeln!(&mut file, "D {key}").unwrap(),
                Operation::Read(key) => writeln!(&mut file, "R {key}").unwrap(),
            }
        }
    }

    operations
}

pub fn read_test_cases_from_file(file_path: &str) -> Vec<Operation> {
    let mut operations = vec![];
    let file = OpenOptions::new().read(true).open(file_path).unwrap();
    let mut reader = BufReader::new(file);
    loop {
        let mut buf = String::new();
        let bytes_read = reader.read_line(&mut buf).unwrap();
        if bytes_read == 0 {
            break;
        }
        // remove \n
        let _ = buf.split_off(buf.len() - 1);
        let (op, rest) = buf.split_once(" ").unwrap();
        match op {
            "S" => {
                let (key, value) = rest.split_once(" ").unwrap();
                operations.push(Operation::Set(key.to_string(), value.to_string()))
            }
            "D" => operations.push(Operation::Delete(rest.to_string())),
            "R" => operations.push(Operation::Read(rest.to_string())),
            op_str => panic!("invalid operation: {op_str}"),
        }
    }
    operations
}
