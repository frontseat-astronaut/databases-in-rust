use std::collections::VecDeque;

use in_memory_db::InMemoryDb;
use kvdb::KVDb;
use log_db::LogDb;
use log_with_index_db::LogWithIndexDb;
use segmented_logs_with_indices_db::SegmentedLogsWithIndicesDb;
use sstable::SSTable;
use test::{correctness_test::CorrectnessTest, latency_test::LatencyTest, Test};

mod error;
mod in_memory_db;
mod kv_file;
mod kvdb;
mod log_db;
mod log_with_index_db;
mod segmented_files_db;
mod segmented_logs_with_indices_db;
mod sstable;
mod test;
mod utils;

fn main() {
    let mut dbs: VecDeque<Box<dyn KVDb>> = VecDeque::new();
    dbs.push_back(Box::new(InMemoryDb::new()));
    dbs.push_back(Box::new(LogDb::new("db_files/log_db/", "log.txt")));
    dbs.push_back(Box::new(
        LogWithIndexDb::new("db_files/log_with_index_db/", "log.txt").unwrap(),
    ));

    for merge_threshold in 2..=5 {
        for size_threshold in 1..=5 {
            dbs.push_back(Box::new(
                SegmentedLogsWithIndicesDb::new(
                    &format!(
                        "db_files/segmented_logs_with_indices_db_{}_{}/",
                        merge_threshold, size_threshold
                    ),
                    500 * size_threshold,
                    merge_threshold,
                )
                .unwrap(),
            ));
        }
    }

    for merging_threshold in 2..5 {
        for sparsity in 1..3 {
            for memtable_size_threshold in 1..=2 {
                dbs.push_back(Box::new(
                    SSTable::new(
                        &format!(
                            "db_files/sstable_{}_{}_{}/",
                            merging_threshold, sparsity, memtable_size_threshold
                        ),
                        merging_threshold,
                        100 * sparsity,
                        2000 * memtable_size_threshold,
                    )
                    .unwrap(),
                ));
            }
        }
    }

    let correctness_test_suite = CorrectnessTest::new(5000, 10000, 0.5, 0.8);
    print!("\n\n");
    while !dbs.is_empty() {
        let mut db = dbs.pop_front().unwrap();
        correctness_test_suite.run(&mut db);
        print!("\n\n");
    }

    // let latency_test_suite = LatencyTest::new(10000, 10000, 0.5, 0.8);
    // print!("\n\n");
    // while !dbs.is_empty() {
    //     let mut db = dbs.pop_front().unwrap();
    //     latency_test_suite.run(&mut db);
    //     print!("\n\n");
    // }
}
