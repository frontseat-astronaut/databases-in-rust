use std::{
    fs::File,
    io::{BufReader, Seek, SeekFrom},
};

use crate::error::DbResult;

use super::{utils::read_line, KVLine};

#[derive(Debug)]
pub enum KVFileIterator<'a> {
    Stopped,
    Running(BufReader<&'a mut File>),
}

impl<'a> Iterator for KVFileIterator<'a> {
    type Item = DbResult<KVLine>;

    fn next(&mut self) -> Option<Self::Item> {
        let Self::Running(reader) = self else {
            return None;
        };
        let Ok(offset) = reader.stream_position() else {
            return None;
        };
        match read_line(reader) {
            Ok(None) => {
                *self = Self::Stopped;
                None
            }
            Ok(Some((key, status))) => Some(Ok(KVLine {
                key,
                status,
                offset,
            })),
            Err(e) => {
                *self = Self::Stopped;
                Some(Err(e))
            }
        }
    }
}

impl<'a> KVFileIterator<'a> {
    pub fn new(file: &'a mut File, offset: u64) -> DbResult<KVFileIterator<'a>> {
        file.seek(SeekFrom::Start(offset))?;
        Ok(Self::Running(BufReader::new(file)))
    }
    pub fn try_next(&mut self) -> DbResult<Option<KVLine>> {
        match self.next() {
            None => Ok(None),
            Some(Ok(inner)) => Ok(Some(inner)),
            Some(Err(e)) => Err(e),
        }
    }
}
