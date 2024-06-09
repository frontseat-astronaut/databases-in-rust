use std::{collections::VecDeque, fs};

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

fn prepare_dbs(include_log_db: bool, include_all_variants: bool) -> VecDeque<Box<dyn KVDb>> {
    let _ = fs::remove_dir_all("./db_files/");

    let mut dbs: VecDeque<Box<dyn KVDb>> = VecDeque::new();
    dbs.push_back(Box::new(InMemoryDb::new()));
    if include_log_db {
        // too slow
        dbs.push_back(Box::new(LogDb::new("db_files/log_db/", "log.txt")));
    }
    dbs.push_back(Box::new(
        LogWithIndexDb::new("db_files/log_with_index_db/", "log.txt").unwrap(),
    ));
    if include_all_variants {
        for merge_threshold in (1..10).step_by(4) {
            for size_threshold in (100..60000).step_by(20000) {
                dbs.push_back(Box::new(
                    SegmentedLogsWithIndicesDb::new(
                        &format!(
                            "db_files/segmented_logs_with_indices_db_{}_{}/",
                            merge_threshold, size_threshold
                        ),
                        size_threshold,
                        merge_threshold,
                    )
                    .unwrap(),
                ));
            }
        }
        for merging_threshold in (2..10).step_by(4) {
            for sparsity in (100..=1000).step_by(400) {
                for memtable_size_threshold in (1000..=10000).step_by(4000) {
                    dbs.push_back(Box::new(
                        SSTable::new(
                            &format!(
                                "db_files/sstable_{}_{}_{}/",
                                merging_threshold, sparsity, memtable_size_threshold
                            ),
                            merging_threshold,
                            sparsity,
                            memtable_size_threshold,
                        )
                        .unwrap(),
                    ));
                }
            }
        }
    } else {
        dbs.push_back(Box::new(
            SegmentedLogsWithIndicesDb::new(
                "db_files/segmented_logs_with_indices_db/",
                1000,
                10000,
            )
            .unwrap(),
        ));
        dbs.push_back(Box::new(
            SSTable::new("db_files/sstable/", 5, 500, 1000).unwrap(),
        ));
    }
    dbs
}

fn run_test_suite<T: Test>(test_suite: T, mut dbs: VecDeque<Box<dyn KVDb>>) {
    print!("\n\n");
    while !dbs.is_empty() {
        let mut db = dbs.pop_front().unwrap();
        let _ = fs::remove_dir_all("./db_files/");
        test_suite.run(&mut db);
        print!("\n\n");
    }
}

fn main() {
    /* CORRECTNESS TESTS */
    let correctness_test_suite = CorrectnessTest::new(20000, 100000, 0.5, 0.8, 0.9);
    let dbs = prepare_dbs(false, false);
    run_test_suite(correctness_test_suite, dbs);

    /* LATENCY TESTS */
    let latency_test_suite = LatencyTest::new(50000, 20000, 0.5, 0.7, 0.8);
    let dbs = prepare_dbs(false, true);
    run_test_suite(latency_test_suite, dbs);

    /* TESTS WITH LESSER NUMBER OF KEYS (because log db is so slow) */
    let correctness_test_suite = CorrectnessTest::new(2000, 10000, 0.5, 0.7, 0.9);
    let dbs = prepare_dbs(true, false);
    run_test_suite(correctness_test_suite, dbs);

    let latency_test_suite = LatencyTest::new(2000, 10000, 0.5, 0.7, 0.8);
    let dbs = prepare_dbs(true, false);
    run_test_suite(latency_test_suite, dbs);
}
