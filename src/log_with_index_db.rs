use crate::kv_file::KVFile;
use crate::{
    in_memory_db::InMemoryDb,
    kvdb::{error::Error, KVDb},
};

pub struct LogWithIndexDb {
    file: KVFile,
    index: InMemoryDb<u64>,
}

impl KVDb for LogWithIndexDb {
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.file
            .append_line(key, Some(value))
            .and_then(|offset| Ok(self.index.set(key, &offset)))
    }
    fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.file
            .append_line(key, None)
            .and_then(|_| Ok(self.index.delete(key)))
    }
    fn get(&self, key: &str) -> Result<Option<String>, Error> {
        match self.index.get(key) {
            Some(offset) => self.file.get_at_offset(offset),
            None => Ok(None),
        }
    }
}

impl LogWithIndexDb {
    pub fn new(dir_path: &str, file_name: &str) -> Result<LogWithIndexDb, Error> {
        let mut index = InMemoryDb::new();
        let file = KVFile::new(dir_path, file_name);
        file.read_lines(&mut |parsed_key, parsed_value, offset| {
            match parsed_value {
                Some(_) => index.set(&parsed_key, &offset),
                None => index.delete(&parsed_key),
            };
            Ok(())
        })
        .and(Ok(LogWithIndexDb { file, index }))
    }
}
