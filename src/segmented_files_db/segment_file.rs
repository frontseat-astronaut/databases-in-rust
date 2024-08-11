use crate::error::DbResult;
use crate::kvdb::KeyStatus;

pub trait SegmentReader<'a> {
    fn get_status(&mut self, key: &str) -> DbResult<Option<KeyStatus<String>>>;
}

pub trait SegmentFile {
    type Reader<'a>: SegmentReader<'a>;

    fn get_status(&mut self, key: &str) -> DbResult<Option<KeyStatus<String>>>;
    fn ready_to_be_archived(&self) -> DbResult<bool> {
        Ok(true)
    }

    fn set_status(&mut self, key: &str, status: &KeyStatus<String>) -> DbResult<()>;
    fn absorb<'a>(&mut self, other: &mut Self::Reader<'a>) -> DbResult<()>;
    fn rename(&mut self, new_file_name: &str) -> DbResult<()>;

    fn delete(self) -> DbResult<()>;
}

pub trait SegmentReaderFactory<F: SegmentFile> {
    fn new<'a>(&self, file: &'a F) -> DbResult<F::Reader<'a>>;
}

pub trait SegmentFileFactory<F: SegmentFile> {
    fn new(&self, file_name: &str) -> DbResult<F>;
    fn from_disk(&self, file_name: &str) -> DbResult<F>;
}
