use crate::error::Error;
use crate::kv_file::KVFile;
use crate::kvdb::{KVDb, KVEntry};

pub struct LogDb {
    file: KVFile,
}

impl KVDb for LogDb {
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.file
            .append_line(key, &KVEntry::Present(value.to_owned()))
            .and(Ok(()))
    }
    fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.file.append_line(key, &KVEntry::Deleted).and(Ok(()))
    }
    fn get(&self, key: &str) -> Result<Option<String>, Error> {
        let mut value = None;
        for line_result in self.file.iter()? {
            let line = line_result?;
            if line.key == key {
                value = line.entry.into();
            }
        }
        Ok(value)
    }
}

impl LogDb {
    pub fn new(dir_path: &str, file_name: &str) -> LogDb {
        LogDb {
            file: KVFile::new(dir_path, file_name),
        }
    }
}
