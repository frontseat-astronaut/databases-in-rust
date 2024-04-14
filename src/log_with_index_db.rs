use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Seek, SeekFrom};
use crate::{in_memory_db::InMemoryDb, kvdb::{error::Error, KVDb}, log_db::{read_line, read_lines, write_line}};

pub struct LogWithIndexDb {
    index: InMemoryDb<u64>,
}

const DIR_PATH: &str = "db_files/log_with_index_db/";
const FILE_NAME: &str = "log.txt";

impl KVDb for LogWithIndexDb {
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.append_to_file(key, value).and_then(|offset| {
            self.index.set(key, &offset)
        })
    }
    fn get(&self, key: &str) -> Result<Option<String>, Error> {
        match self.index.get(key) {
            Ok(Some(offset)) => {
                println!("[log] found offset {} for key {} in index", offset, key);
                self.get_at_offset(offset)
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        } 
    }
}

impl LogWithIndexDb {
    pub fn new() -> Result<LogWithIndexDb, Error> {
        let mut index = InMemoryDb::new();
        open_file()
            .and_then(|mut file| {
                read_lines(&mut file, &mut |parsed_key, _, offset| {
                    index.set(&parsed_key, &offset)
                }).and(Ok(LogWithIndexDb{
                    index,
                }))
            })
    }

    fn append_to_file(&mut self, key: &str, value: &str) -> Result<u64, Error> {
        open_file()
            .and_then(|mut file| {
                match file.seek(SeekFrom::End(0)) {
                    Ok(pos) => {
                        match write_line(&mut file, key, value) {
                            Ok(()) => {
                                Ok(pos)
                            }
                            Err(e) => {
                                Err(e)
                            }
                        }
                    }
                    Err(e) => {
                        Err(Error::from_io_error(&e))
                    }
                }
            })
    }

    fn get_at_offset(&self, offset: u64) -> Result<Option<String>, Error> {
        open_file()
            .and_then(|mut file| {
                // move file pointer
                if let Err(e) = file.seek(SeekFrom::Start(offset)) {
                    return Err(Error::from_io_error(&e))
                }
                let mut reader = BufReader::new(&mut file);
                match read_line(&mut reader) {
                    Ok(None) => Ok(None),
                    Ok(Some((_, value))) => {
                        Ok(Some(value))
                    }
                    Err(e) => Err(e),
                }
            })
    }
}

fn open_file() -> Result<File, Error> {
    if let Err(e) = fs::create_dir_all(DIR_PATH) {
        return Err(Error::from_io_error(&e))
    }
    let file_path = DIR_PATH.to_owned() + FILE_NAME;
    OpenOptions::new()
        .read(true)
        .write(true)
        .append(true)
        .create(true)
        .open(file_path).map_err(|e| {
            Error::from_io_error(&e)
        })
}