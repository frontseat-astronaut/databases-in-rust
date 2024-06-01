use in_memory_db::InMemoryDb;
use kvdb::{test::Test, KVDb};
use log_db::LogDb;
use log_with_index_db::LogWithIndexDb;
use segmented_logs_with_indices_db::SegmentedLogsWithIndicesDb;
use sstable::SSTable;

mod error;
mod in_memory_db;
mod kv_file;
mod kvdb;
mod log_db;
mod log_with_index_db;
mod segmented_files_db;
mod segmented_logs_with_indices_db;
mod sstable;
mod utils;

const NUM_KEYS: u32 = 100;
const NUM_OPERATIONS: u32 = 1000000;
const READ_WRITE_RATIO: f32 = 0.995;

fn main() {
    let mut dbs: Vec<Box<dyn KVDb>> = vec![
        Box::new(LogWithIndexDb::new("db_files/log_with_index_db/", "log.txt").unwrap()),
        Box::new(LogDb::new("db_files/log_db/", "log.txt")),
        Box::new(InMemoryDb::new()),
    ];

    while !dbs.is_empty() {
        let db = dbs.pop().unwrap();
        println!("-------Running test suite for {}-------", db.name());
        let mut test = Test::new(db);
        test.test_updates(NUM_KEYS, NUM_OPERATIONS, READ_WRITE_RATIO);
        print!("\n\n");
    }

    for merge_threshold in 2..5 {
        for size_threshold in 1..5 {
            println!("Testing Segmented Logs (with indices) DB with size_threshold: {} and merge_threshold: {}", 500*size_threshold, merge_threshold);
            let mut segmented_logs_with_indices_db_test = Test::new(Box::new(
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
            segmented_logs_with_indices_db_test.test_updates(100, 1000000, 0.5);
        }
    }

    let mut sstable_test = Test::new(Box::new(
        SSTable::new("db_files/sstable/", 10, 50, 3).unwrap(),
    ));
    sstable_test.run();
}
