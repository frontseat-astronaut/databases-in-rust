use crate::kv_file::KVFile;
use crate::{in_memory_db::InMemoryDb, kvdb::{error::Error, KVDb}};

pub struct LogWithIndexDb {
    file: KVFile,
    index: InMemoryDb<u64>,
}

const DIR_PATH: &str = "db_files/log_with_index_db/";
const FILE_NAME: &str = "log.txt";

impl KVDb for LogWithIndexDb {
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.file.append_line(key, value).and_then(|offset| {
            self.index.set(key, &offset)
        })
    }
    fn get(&self, key: &str) -> Result<Option<String>, Error> {
        match self.index.get(key) {
            Ok(Some(offset)) => {
                println!("[log] found offset {} for key {} in index", offset, key);
                self.file.get_at_offset(offset)
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        } 
    }
}

impl LogWithIndexDb {
    pub fn new() -> Result<LogWithIndexDb, Error> {
        let mut index = InMemoryDb::new();
        let file = KVFile::new(DIR_PATH, FILE_NAME);
        file.read_lines(&mut |parsed_key, _, offset| {
            index.set(&parsed_key, &offset)
        }).and(Ok(LogWithIndexDb{
            file,
            index,
        }))
    }
}
