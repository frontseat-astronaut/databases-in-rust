use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, ErrorKind, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;

use crate::error::Error;
use crate::kvdb::KVEntry;

use self::utils::{read_line, write_line};

use reader::KVFileReader;

mod reader;
mod utils;

const DELIMITER: &str = ",";
const TOMBSTONE: &str = "🪦";

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

    pub fn get_reader(&self) -> Result<KVFileReader, Error> {
        self.open_file(true, true).and_then(|maybe_file| {
            let file = maybe_file.unwrap();
            Ok(KVFileReader {
                reader: BufReader::new(file),
            })
        })
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

    pub fn read_lines(
        &self,
        process_line: &mut dyn FnMut(String, KVEntry<String>, u64) -> Result<bool, Error>,
    ) -> Result<(), Error> {
        self.read_lines_from_offset(process_line, 0)
    }

    pub fn read_lines_from_offset(
        &self,
        process_line: &mut dyn FnMut(String, KVEntry<String>, u64) -> Result<bool, Error>,
        offset: u64,
    ) -> Result<(), Error> {
        self.open_file(true, false).and_then(|maybe_file| {
            let Some(mut file) = maybe_file else {
                return Ok(());
            };
            if offset > 0 {
                file.seek(SeekFrom::Start(offset))?;
            }
            let mut reader = BufReader::new(&mut file);
            loop {
                let offset = reader.stream_position()?;
                match read_line(&mut reader)? {
                    Some((key, value)) => {
                        let stop = process_line(key, value, offset)?;
                        if stop {
                            break;
                        }
                    }
                    None => break,
                }
            }
            Ok(())
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
        self.read_lines_from_offset(
            &mut |_, entry, _| {
                value = entry.into();
                Ok(true)
            },
            offset,
        )?;
        Ok(value)
    }

    pub fn delete(self) -> Result<(), Error> {
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
