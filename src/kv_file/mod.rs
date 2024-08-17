use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;

use crate::error::{DbResult, Error};
use crate::kvdb::KeyStatus;

use self::iterator::KVFileIterator;
use self::utils::write_line;

mod iterator;
mod utils;

const DELIMITER: &str = ",";
const TOMBSTONE: &str = "🪦";

#[derive(Debug)]
pub struct KVLine {
    pub key: String,
    pub status: KeyStatus<String>,
    pub offset: u64,
}

pub struct KVFile {
    pub dir_path: String,
    pub file_name: String,
    file: Option<File>,
}

impl KVFile {
    pub fn new(dir_path: &str, file_name: &str) -> DbResult<KVFile> {
        Ok(KVFile {
            dir_path: dir_path.to_string(),
            file_name: file_name.to_string(),
            file: None,
        })
    }
    pub fn copy(file: &Self) -> DbResult<KVFile> {
        Self::new(&file.dir_path, &file.file_name)
    }
    pub fn iter(&mut self) -> DbResult<KVFileIterator> {
        self.create_iterator(0)
    }
    pub fn iter_from_offset(&mut self, offset: u64) -> DbResult<KVFileIterator> {
        self.create_iterator(offset)
    }
    pub fn size(&self) -> DbResult<u64> {
        match self.file {
            None => Ok(0),
            Some(ref file) => {
                let metadata = file.metadata()?;
                Ok(metadata.size())
            }
        }
    }
    pub fn append_line(&mut self, key: &str, status: &KeyStatus<String>) -> DbResult<u64> {
        self.open_file()?;
        let file = self.file.as_mut().unwrap();
        let pos = file.seek(SeekFrom::End(0))?;
        write_line(file, key, status).and(Ok(pos))
    }
    pub fn read_at_offset(&mut self, offset: u64) -> DbResult<Option<String>> {
        for line_result in self.iter_from_offset(offset)? {
            let line = line_result?;
            return Ok(line.status.into());
        }
        Ok(None)
    }
    pub fn delete(&mut self) -> DbResult<()> {
        self.close_file()?;

        let file_path = self.get_file_path();
        match fs::remove_file(file_path) {
            Ok(()) => Ok(()),
            Err(ref e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
    pub fn rename(&mut self, new_file_name: &str) -> DbResult<()> {
        self.close_file()?;

        let old_file_path = self.get_file_path();
        self.file_name = new_file_name.to_owned();
        let new_file_path = self.get_file_path();
        match fs::rename(old_file_path, new_file_path) {
            Ok(()) => {}
            Err(ref e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        };

        Ok(())
    }
    fn get_file_path(&self) -> String {
        get_file_path(&self.dir_path, &self.file_name)
    }
    fn open_file(&mut self) -> DbResult<()> {
        if self.file.is_some() {
            return Ok(());
        }
        fs::create_dir_all(&self.dir_path)?;
        let file_path = get_file_path(&self.dir_path, &self.file_name);
        self.file = Some(
            OpenOptions::new()
                .read(true)
                .write(true)
                .append(true)
                .create(true)
                .open(file_path)
                .map_err(Error::from)?,
        );
        Ok(())
    }
    fn close_file(&mut self) -> DbResult<()> {
        if let Some(mut file) = self.file.take() {
            file.flush()?;
        }
        Ok(())
    }
    fn create_iterator(&mut self, offset: u64) -> DbResult<KVFileIterator> {
        self.open_file()?;
        let file = self.file.as_mut().unwrap();
        KVFileIterator::new(file, offset)
    }
}

fn get_file_path(dir_path: &str, file_name: &str) -> String {
    dir_path.to_owned() + file_name
}
