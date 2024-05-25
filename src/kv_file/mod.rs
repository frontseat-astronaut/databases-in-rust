use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;

use crate::error::Error;
use crate::kvdb::KVEntry;

use self::iterator::KVFileIterator;
use self::utils::write_line;

mod iterator;
mod utils;

const DELIMITER: &str = ",";
const TOMBSTONE: &str = "🪦";

pub struct KVLine {
    pub key: String,
    pub entry: KVEntry<String>,
    pub offset: u64,
}

pub struct KVFile {
    dir_path: String,
    pub file_name: String,
}

impl KVFile {
    pub fn new(dir_path: &str, file_name: &str) -> KVFile {
        KVFile {
            dir_path: dir_path.to_string(),
            file_name: file_name.to_string(),
        }
    }
    pub fn iter(&self) -> Result<KVFileIterator, Error> {
        self.open_file(true, false)
            .and_then(|maybe_file| KVFileIterator::new(maybe_file, 0))
    }
    pub fn iter_from_offset(&self, offset: u64) -> Result<KVFileIterator, Error> {
        self.open_file(true, false)
            .and_then(|maybe_file| KVFileIterator::new(maybe_file, offset))
    }
    pub fn size(&self) -> Result<u64, Error> {
        self.open_file(true, false).and_then(|maybe_file| {
            let Some(file) = maybe_file else {
                return Ok(0);
            };
            let metadata = file.metadata()?;
            Ok(metadata.size())
        })
    }
    pub fn append_line(&mut self, key: &str, value: &KVEntry<String>) -> Result<u64, Error> {
        self.open_file(false, true).and_then(|maybe_file| {
            let mut file = maybe_file.unwrap();
            let pos = file.seek(SeekFrom::End(0))?;
            write_line(&mut file, key, value).and(Ok(pos))
        })
    }
    pub fn get_at_offset(&self, offset: u64) -> Result<Option<String>, Error> {
        let mut value = None;
        for line_result in self.iter_from_offset(offset)? {
            let line = line_result?;
            value = line.entry.into();
        }
        Ok(value)
    }
    pub fn delete(&mut self) -> Result<(), Error> {
        let file_path = self.get_file_path();
        match fs::remove_file(file_path) {
            Ok(()) => Ok(()),
            Err(ref e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
    pub fn rename(&mut self, new_file_name: &str) -> Result<(), Error> {
        let old_file_path = self.get_file_path();
        self.file_name = new_file_name.to_owned();
        let new_file_path = self.get_file_path();
        match fs::rename(old_file_path, new_file_path) {
            Ok(()) => Ok(()),
            Err(ref e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
    fn open_file(&self, read: bool, write: bool) -> Result<Option<File>, Error> {
        fs::create_dir_all(&self.dir_path)?;
        let file_path = self.get_file_path();
        match OpenOptions::new()
            .read(read)
            .write(write)
            .append(write)
            .create(write)
            .open(file_path)
        {
            Ok(file) => Ok(Some(file)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    fn get_file_path(&self) -> String {
        self.dir_path.to_owned() + self.file_name.as_str()
    }
}
