use crate::error::DbResult;
use crate::in_memory_db::InMemoryDb;
use crate::kv_file::{KVFile, KVLine};
use crate::kvdb::{KVDb, KeyStatus};

pub struct LogWithIndexDb {
    file: KVFile,
    index: InMemoryDb<u64>,
}

impl KVDb for LogWithIndexDb {
    fn description(&self) -> String {
        "Log with index DB".to_string()
    }
    fn set(&mut self, key: &str, value: &str) -> DbResult<()> {
        self.file
            .append_line(key, &KeyStatus::Present(value.to_string()))
            .and_then(|offset| Ok(self.index.set(key, &offset)))
    }
    fn delete(&mut self, key: &str) -> DbResult<()> {
        self.file
            .append_line(key, &KeyStatus::Deleted)
            .and_then(|_| Ok(self.index.delete(key)))
    }
    fn get(&mut self, key: &str) -> DbResult<Option<String>> {
        match self.index.get(key) {
            Some(offset) => self.file.read_at_offset(offset),
            None => Ok(None),
        }
    }
}

impl LogWithIndexDb {
    pub fn new(dir_path: &str, file_name: &str) -> DbResult<LogWithIndexDb> {
        let mut index = InMemoryDb::new();
        let mut file = KVFile::new(dir_path, file_name)?;
        for line_result in file.iter()? {
            let KVLine {
                key,
                status,
                offset,
            } = line_result?;
            match status {
                KeyStatus::Present(_) => index.set(&key, &offset),
                KeyStatus::Deleted => index.delete(&key),
            };
        }
        Ok(LogWithIndexDb { file, index })
    }
}
