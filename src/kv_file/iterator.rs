use std::{
    fs::File,
    io::{BufReader, Seek, SeekFrom},
};

use crate::error::Error;

use super::{utils::read_line, KVLine};

#[derive(Debug)]
pub enum KVFileIterator {
    Stopped,
    Running(BufReader<File>),
}

impl Iterator for KVFileIterator {
    type Item = Result<KVLine, Error>;

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
            Ok(Some((key, entry))) => Some(Ok(KVLine { key, entry, offset })),
            Err(e) => {
                *self = Self::Stopped;
                Some(Err(e))
            }
        }
    }
}

impl KVFileIterator {
    pub fn new(maybe_file: Option<File>, offset: u64) -> Result<KVFileIterator, Error> {
        match maybe_file {
            None => Ok(Self::Stopped),
            Some(mut file) => {
                if offset > 0 {
                    file.seek(SeekFrom::Start(offset))?;
                }
                Ok(Self::Running(BufReader::new(file)))
            }
        }
    }
    pub fn try_next(&mut self) -> Result<Option<KVLine>, Error> {
        match self.next() {
            None => Ok(None),
            Some(Ok(inner)) => Ok(Some(inner)),
            Some(Err(e)) => Err(e),
        }
    }
}
