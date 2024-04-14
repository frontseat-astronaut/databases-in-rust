use in_memory_db::InMemoryDb;
use kvdb::test::Test;
use log_db::LogDb;
use log_with_index_db::LogWithIndexDb;

mod kvdb;
mod kv_file;
mod in_memory_db;
mod log_db;
mod log_with_index_db;

fn main() {
    let mut in_mem_db_test = Test::new(InMemoryDb::new());
    in_mem_db_test.run();

    let mut log_db_test = Test::new(LogDb::new());
    log_db_test.run();

    let mut log_with_index_db_test = Test::new(LogWithIndexDb::new().unwrap());
    log_with_index_db_test.run();
}
