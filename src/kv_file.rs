use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;

use crate::error::Error;
use crate::kvdb::KVEntry;

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
                match Self::read_line(&mut reader)? {
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
            self.write_line(&mut file, key, value).and(Ok(pos))
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
        fs::remove_file(file_path)?;
        Ok(())
    }

    pub fn rename(&mut self, new_file_name: &str) -> Result<(), Error> {
        let old_file_path = self.get_file_path();
        self.file_name = new_file_name.to_owned();
        let new_file_path = self.get_file_path();
        fs::rename(old_file_path, new_file_path)?;
        Ok(())
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

    fn read_line(
        reader: &mut BufReader<&mut File>,
    ) -> Result<Option<(String, KVEntry<String>)>, Error> {
        let mut buf = String::new();
        let bytes_read = reader.read_line(&mut buf)?;
        if bytes_read == 0 {
            return Ok(None);
        }
        // remove \n
        let _ = buf.split_off(buf.len() - 1);
        match buf.split_once(DELIMITER) {
            Some((key, read_value)) => {
                let mut value = KVEntry::Deleted;
                if read_value != TOMBSTONE {
                    value = KVEntry::Present(read_value.to_string())
                }
                Ok(Some((key.to_string(), value)))
            }
            None => Err(Error::InvalidData(format!(
                "ill-formed line in file, expected the delimiter '{}' to be present",
                DELIMITER
            ))),
        }
    }

    fn write_line(&self, file: &mut File, key: &str, value: &KVEntry<String>) -> Result<(), Error> {
        if key.contains(DELIMITER) {
            return Err(Error::InvalidInput(format!(
                "key must not have '{}'",
                DELIMITER
            )));
        }
        let written_value = match value {
            KVEntry::Present(ref value) => {
                if value == TOMBSTONE {
                    return Err(Error::InvalidInput(format!(
                        "storing {} as a value is not supported",
                        TOMBSTONE
                    )));
                }
                value
            }
            KVEntry::Deleted => TOMBSTONE,
        };
        writeln!(file, "{}{}{}", key, DELIMITER, written_value)?;
        Ok(())
    }

    fn get_file_path(&self) -> String {
        self.dir_path.to_owned() + self.file_name.as_str()
    }
}
