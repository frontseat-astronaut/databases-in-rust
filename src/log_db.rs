use crate::kv_file::KVFile;
use crate::kvdb::{KVDb, error::Error};

pub struct LogDb {
    file: KVFile
}

const DIR_PATH: &str = "db_files/log_db/";
const FILE_NAME: &str = "log.txt";

impl KVDb for LogDb {
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.file.append_line(key, value).and(Ok(()))
    }

    fn get(&self, key: &str) -> Result<Option<String>, Error> {
        let mut value = None;
        self.file.read_lines(&mut |parsed_key, parsed_value, _| {
            if parsed_key == key {
                value = Some(parsed_value);
            }
            Ok(())
        }).and(Ok(value))
    }
}

impl LogDb {
    pub fn new() -> LogDb {
        LogDb {
            file: KVFile::new(DIR_PATH, FILE_NAME),
        }
    }
}