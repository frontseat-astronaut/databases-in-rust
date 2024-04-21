use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};

use crate::kvdb::error::Error;

const DELIMITER: &str = ",";
const TOMBSTONE: &str = "🪦";

#[derive(Clone)]
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
        self.open_file().and_then(|mut file| {
            let mut reader = BufReader::new(&mut file);
            loop {
                let result = match reader.stream_position() {
                    Ok(offset) => match Self::read_line(&mut reader) {
                        Ok(None) => break,
                        Ok(Some((key, value))) => process_line(key, value, offset),
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(Error::from_io_error(&e)),
                };
                if result.is_err() {
                    return result;
                }
            }
            Ok(())
        })
    }

    pub fn append_line(&mut self, key: &str, value: Option<&str>) -> Result<u64, Error> {
        self.open_file()
            .and_then(|mut file| match file.seek(SeekFrom::End(0)) {
                Ok(pos) => match self.write_line(key, value) {
                    Ok(()) => Ok(pos),
                    Err(e) => Err(e),
                },
                Err(e) => Err(Error::from_io_error(&e)),
            })
    }

    pub fn get_at_offset(&self, offset: u64) -> Result<Option<String>, Error> {
        self.open_file().and_then(|mut file| {
            if let Err(e) = file.seek(SeekFrom::Start(offset)) {
                return Err(Error::from_io_error(&e));
            }
            let mut reader = BufReader::new(&mut file);
            match Self::read_line(&mut reader) {
                Ok(None) => Ok(None),
                Ok(Some((_, value))) => Ok(value),
                Err(e) => Err(e),
            }
        })
    }

    fn open_file(&self) -> Result<File, Error> {
        if let Err(e) = fs::create_dir_all(&self.dir_path) {
            return Err(Error::from_io_error(&e));
        }
        let file_path = self.get_file_path();
        OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)
            .map_err(|e| Error::from_io_error(&e))
    }

    fn read_line(
        reader: &mut BufReader<&mut File>,
    ) -> Result<Option<(String, Option<String>)>, Error> {
        let mut buf = String::new();
        match reader.read_line(&mut buf) {
            Ok(0) => Ok(None),
            Ok(_) => {
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
            Err(e) => Err(Error::from_io_error(&e)),
        }
    }

    fn write_line(&self, key: &str, value: Option<&str>) -> Result<(), Error> {
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
        self.open_file().and_then(|mut file| {
            if let Err(e) = writeln!(&mut file, "{}{}{}", key, DELIMITER, written_value) {
                return Err(Error::from_io_error(&e));
            }
            Ok(())
        })
    }

    pub fn delete(self) -> Result<(), Error> {
        let file_path = self.get_file_path();
        fs::remove_file(file_path).map_err(|e| Error::from_io_error(&e))
    }

    pub fn rename(&mut self, new_file_name: &str) -> Result<(), Error> {
        let old_file_path = self.get_file_path();
        self.file_name = new_file_name.to_owned();
        let new_file_path = self.get_file_path();
        fs::rename(old_file_path, new_file_path).map_err(|e| Error::from_io_error(&e))
    }

    fn get_file_path(&self) -> String {
        self.dir_path.to_owned() + self.file_name.as_str()
    }
}
