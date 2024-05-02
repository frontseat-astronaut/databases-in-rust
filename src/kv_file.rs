use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom, Write};

use crate::kvdb::error::Error;
use crate::{unwrap_or_return, unwrap_or_return_io_error};

const DELIMITER: &str = ",";
const TOMBSTONE: &str = "🪦";

pub struct KVFile {
    dir_path: String,
    file_name: String,
}

impl KVFile {
    pub fn new(dir_path: &str, file_name: &str) -> KVFile {
        KVFile {
            dir_path: dir_path.to_string(),
            file_name: file_name.to_string(),
        }
    }

    pub fn count_lines(&self) -> Result<u64, Error> {
        let mut count = 0;
        self.read_lines(&mut |_, _, _| {
            count += 1;
            Ok(())
        })
        .and(Ok(count))
    }

    pub fn read_lines(
        &self,
        process_line: &mut dyn FnMut(String, Option<String>, u64) -> Result<(), Error>,
    ) -> Result<(), Error> {
        self.open_file(true, false).and_then(|maybe_file| {
            let Some(mut file) = maybe_file else {
                return Ok(());
            };
            let mut reader = BufReader::new(&mut file);
            loop {
                let offset = unwrap_or_return_io_error!(reader.stream_position());
                match unwrap_or_return!(Self::read_line(&mut reader)) {
                    Some((key, value)) => {
                        unwrap_or_return!(process_line(key, value, offset))
                    }
                    None => break,
                }
            }
            Ok(())
        })
    }

    pub fn append_line(&mut self, key: &str, value: Option<&str>) -> Result<u64, Error> {
        self.open_file(false, true).and_then(|maybe_file| {
            let mut file = maybe_file.unwrap();
            let pos = unwrap_or_return_io_error!(file.seek(SeekFrom::End(0)));
            self.write_line(&mut file, key, value).and(Ok(pos))
        })
    }

    pub fn get_at_offset(&self, offset: u64) -> Result<Option<String>, Error> {
        self.open_file(true, false).and_then(|maybe_file| {
            let Some(mut file) = maybe_file else {
                return Ok(None);
            };
            unwrap_or_return_io_error!(file.seek(SeekFrom::Start(offset)));
            let mut reader = BufReader::new(&mut file);
            Self::read_line(&mut reader).map(|maybe_kv| maybe_kv.and_then(|(_, value)| value))
        })
    }

    pub fn delete(self) -> Result<(), Error> {
        let file_path = self.get_file_path();
        fs::remove_file(file_path).map_err(|e| Error::from_io_error(e))
    }

    pub fn rename(&mut self, new_file_name: &str) -> Result<(), Error> {
        let old_file_path = self.get_file_path();
        self.file_name = new_file_name.to_owned();
        let new_file_path = self.get_file_path();
        fs::rename(old_file_path, new_file_path).map_err(|e| Error::from_io_error(e))
    }

    fn open_file(&self, read: bool, write: bool) -> Result<Option<File>, Error> {
        unwrap_or_return_io_error!(fs::create_dir_all(&self.dir_path));
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
            Err(e) => Err(Error::from_io_error(e)),
        }
    }

    fn read_line(
        reader: &mut BufReader<&mut File>,
    ) -> Result<Option<(String, Option<String>)>, Error> {
        let mut buf = String::new();
        let bytes_read = unwrap_or_return_io_error!(reader.read_line(&mut buf));
        if bytes_read == 0 {
            return Ok(None);
        }
        // remove \n
        let _ = buf.split_off(buf.len() - 1);
        match buf.split_once(DELIMITER) {
            Some((key, read_value)) => {
                let mut value = None;
                if read_value != TOMBSTONE {
                    value = Some(read_value.to_string())
                }
                Ok(Some((key.to_string(), value)))
            }
            None => Err(Error::new(&format!(
                "ill-formed line in file, expected the delimiter '{}' to be present",
                DELIMITER
            ))),
        }
    }

    fn write_line(&self, file: &mut File, key: &str, value: Option<&str>) -> Result<(), Error> {
        if key.contains(DELIMITER) {
            return Err(Error::new(&format!("key must not have '{}'", DELIMITER)));
        }
        let written_value = match value {
            Some(TOMBSTONE) => {
                return Err(Error::new(&format!(
                    "storing {} as a value is not supported",
                    TOMBSTONE
                )))
            }
            Some(value) => value,
            None => TOMBSTONE,
        };
        unwrap_or_return_io_error!(writeln!(file, "{}{}{}", key, DELIMITER, written_value));
        Ok(())
    }

    fn get_file_path(&self) -> String {
        self.dir_path.to_owned() + self.file_name.as_str()
    }
}
