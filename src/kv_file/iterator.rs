use std::{
    fs::File,
    io::{BufReader, Seek, SeekFrom},
};

use crate::{
    error::{DbResult, Error},
    kvdb::KeyStatus,
    serializers::{Serializer, SerializerEnum},
};

use super::{KVLine, KVRecord};

pub enum KVFileIterator<'a> {
    Stopped,
    Running(&'a SerializerEnum, BufReader<&'a mut File>),
}

impl<'a> Iterator for KVFileIterator<'a> {
    type Item = DbResult<KVLine>;

    fn next(&mut self) -> Option<Self::Item> {
        let Self::Running(serializer, reader) = self else {
            return None;
        };
        let Ok(offset) = reader.stream_position() else {
            return None;
        };
        if let Ok(end_pos) = reader.get_ref().metadata().map(|m| m.len()) {
            if offset >= end_pos {
                return None;
            }
        }
        match serializer.read::<KVRecord, &mut File>(reader) {
            Ok(None) => {
                *self = Self::Stopped;
                None
            }
            Ok(Some(record)) => Some(Ok(KVLine {
                key: record.key,
                status: if record.is_present {
                    KeyStatus::Present(record.value)
                } else {
                    KeyStatus::Deleted
                },
                offset,
            })),
            Err(e) => {
                *self = Self::Stopped;
                Some(Err(Error::wrap(
                    "error in reading next entry in KV file",
                    e,
                )))
            }
        }
    }
}

impl<'a> KVFileIterator<'a> {
    pub fn new(serializer: &'a SerializerEnum, file: &'a mut File, offset: u64) -> DbResult<Self> {
        file.seek(SeekFrom::Start(offset))?;
        Ok(Self::Running(serializer, BufReader::new(file)))
    }
    pub fn try_next(&mut self) -> DbResult<Option<KVLine>> {
        match self.next() {
            None => Ok(None),
            Some(Ok(inner)) => Ok(Some(inner)),
            Some(Err(e)) => Err(e),
        }
    }
}
