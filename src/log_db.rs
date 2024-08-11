use crate::error::DbResult;
use crate::kv_file::KVFile;
use crate::kvdb::{KVDb, KeyStatus};

pub struct LogDb {
    file: KVFile,
}

impl KVDb for LogDb {
    fn description(&self) -> String {
        "Log DB".to_string()
    }
    fn set(&mut self, key: &str, value: &str) -> DbResult<()> {
        self.file
            .append_line(key, &KeyStatus::Present(value.to_owned()))
            .and(Ok(()))
    }
    fn delete(&mut self, key: &str) -> DbResult<()> {
        self.file.append_line(key, &KeyStatus::Deleted).and(Ok(()))
    }
    fn get(&mut self, key: &str) -> DbResult<Option<String>> {
        let mut value = None;
        for line_result in self.file.iter()? {
            let line = line_result?;
            if line.key == key {
                value = line.status.into();
            }
        }
        Ok(value)
    }
}

impl LogDb {
    pub fn new(dir_path: &str, file_name: &str) -> DbResult<LogDb> {
        Ok(LogDb {
            file: KVFile::new(dir_path, file_name)?,
        })
    }
}
