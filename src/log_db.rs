use crate::error::Error;
use crate::kv_file::KVFile;
use crate::kvdb::KVDb;

pub struct LogDb {
    file: KVFile,
}

impl KVDb for LogDb {
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.file.append_line(key, Some(value)).and(Ok(()))
    }

    fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.file.append_line(key, None).and(Ok(()))
    }

    fn get(&self, key: &str) -> Result<Option<String>, Error> {
        let mut value = None;
        self.file
            .read_lines(&mut |parsed_key, parsed_value, _| {
                if parsed_key == key {
                    value = parsed_value;
                }
                Ok(())
            })
            .and(Ok(value))
    }
}

impl LogDb {
    pub fn new(dir_path: &str, file_name: &str) -> LogDb {
        LogDb {
            file: KVFile::new(dir_path, file_name),
        }
    }
}
