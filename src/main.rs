use in_memory_db::InMemoryDb;
use kvdb::test::Test;
use log_db::LogDb;
use log_with_index_db::LogWithIndexDb;
use segmented_logs_with_indices_db::SegmentedLogsWithIndicesDb;

mod in_memory_db;
mod kv_file;
mod kvdb;
mod log_db;
mod log_with_index_db;
mod segmented_logs_with_indices_db;

fn main() {
    let mut in_mem_db_test = Test::new(InMemoryDb::new());
    in_mem_db_test.run();

    let mut log_db_test = Test::new(LogDb::new("db_files/log_db/", "log.txt"));
    log_db_test.run();

    let mut log_with_index_db_test =
        Test::new(LogWithIndexDb::new("db_files/log_with_index_db/", "log.txt").unwrap());
    log_with_index_db_test.run();

    let mut segmented_logs_with_indices_db_test = Test::new(
        SegmentedLogsWithIndicesDb::new("db_files/segmented_logs_with_indices_db/", 3).unwrap(),
    );
    segmented_logs_with_indices_db_test.run();
}
