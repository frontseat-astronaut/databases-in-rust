use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, ErrorKind, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;

use serde::{Deserialize, Serialize};

use crate::error::{DbResult, Error};
use crate::kvdb::KeyStatus;
use crate::serializers::{self, Serializer, SerializerEnum};

use self::iterator::KVFileIterator;

mod iterator;

#[derive(Debug)]
pub struct KVLine {
    pub key: String,
    pub status: KeyStatus<String>,
    pub offset: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct KVRecord {
    is_present: bool,
    key: String,
    value: String,
}

#[derive(Copy, Clone)]
pub enum KVFileSerializerOption {
    Avro,
    MessagePack,
    JSON,
}

pub struct KVFile {
    pub dir_path: String,
    pub file_name: String,
    pub serializer_option: KVFileSerializerOption,
    serializer: SerializerEnum,
    file: Option<File>,
}

impl KVFile {
    pub fn new(
        dir_path: &str,
        file_name: &str,
        serializer_option: KVFileSerializerOption,
    ) -> DbResult<Self> {
        let serializer = match serializer_option {
            KVFileSerializerOption::Avro => SerializerEnum::Avro(serializers::AvroSerializer::new(
                "./src/kv_file/kvfile.avsc",
            )?),
            KVFileSerializerOption::MessagePack => {
                SerializerEnum::MessagePack(serializers::MessagePackSerializer::new())
            }
            KVFileSerializerOption::JSON => {
                SerializerEnum::JSON(serializers::JsonSerializer::new())
            }
        };
        Ok(KVFile {
            dir_path: dir_path.to_string(),
            file_name: file_name.to_string(),
            file: None,
            serializer_option,
            serializer,
        })
    }
    pub fn copy(file: &Self) -> DbResult<Self> {
        Ok(KVFile {
            dir_path: file.dir_path.to_string(),
            file_name: file.file_name.to_string(),
            file: None,
            serializer_option: file.serializer_option,
            serializer: file.serializer.copy()?,
        })
    }
    pub fn iter(&mut self) -> DbResult<KVFileIterator> {
        self.create_iterator(0)
    }
    pub fn iter_from_offset(&mut self, offset: u64) -> DbResult<KVFileIterator> {
        self.create_iterator(offset)
    }
    pub fn size(&mut self) -> DbResult<u64> {
        self.open_file()?;
        let metadata = self.file.as_mut().unwrap().metadata()?;
        Ok(metadata.size())
    }
    pub fn append_line(&mut self, key: &str, status: &KeyStatus<String>) -> DbResult<u64> {
        self.open_file()?;
        let file = self.file.as_mut().unwrap();
        let pos = file.seek(SeekFrom::End(0))?;
        let record = match status {
            KeyStatus::Deleted => KVRecord {
                is_present: false,
                key: key.to_string(),
                value: "".to_string(),
            },
            KeyStatus::Present(value) => KVRecord {
                is_present: true,
                key: key.to_string(),
                value: value.to_string(),
            },
        };
        self.serializer.write(record, &mut BufWriter::new(file))?;
        Ok(pos)
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
        if new_file_name.eq(&self.file_name) {
            return Ok(());
        }
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
        KVFileIterator::new(&mut self.serializer, file, offset)
    }
}

fn get_file_path(dir_path: &str, file_name: &str) -> String {
    dir_path.to_owned() + file_name
}
