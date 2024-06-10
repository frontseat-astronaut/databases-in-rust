use crate::{error::Error, kvdb::KeyStatus};

pub trait SegmentReader<'a> {
    fn get_status(&mut self, key: &str) -> Result<Option<KeyStatus<String>>, Error>;
}

pub trait SegmentFile {
    type Reader<'a>: SegmentReader<'a>;

    fn get_status(&mut self, key: &str) -> Result<Option<KeyStatus<String>>, Error>;
    fn ready_to_be_archived(&self) -> Result<bool, Error> {
        Ok(true)
    }

    fn set_status(&mut self, key: &str, status: &KeyStatus<String>) -> Result<(), Error>;
    fn absorb<'a>(&mut self, other: &mut Self::Reader<'a>) -> Result<(), Error>;
    fn rename(&mut self, new_file_name: &str) -> Result<(), Error>;

    fn delete(self) -> Result<(), Error>;
}

pub trait SegmentReaderFactory<F: SegmentFile> {
    fn new<'a>(&self, file: &'a F) -> Result<F::Reader<'a>, Error>;
}

pub trait SegmentFileFactory<F: SegmentFile> {
    fn new(&self, file_name: &str) -> Result<F, Error>;
    fn from_disk(&self, file_name: &str) -> Result<F, Error>;
}
