use crate::{error::Error, kvdb::KeyStatus};

pub trait SegmentFile {
    fn get_status(&self, key: &str) -> Result<Option<KeyStatus<String>>, Error>;
    fn ready_to_be_archived(&self) -> Result<bool, Error> {
        Ok(true)
    }

    fn set_status(&mut self, key: &str, status: &KeyStatus<String>) -> Result<(), Error>;
    fn absorb(&mut self, other: &Self) -> Result<(), Error>;
    fn rename(&mut self, new_file_name: &str) -> Result<(), Error>;

    fn delete(self) -> Result<(), Error>;
}

pub trait SegmentFileFactory<T: SegmentFile> {
    fn new(&self, file_name: &str) -> Result<T, Error>;
    fn from_disk(&self, file_name: &str) -> Result<T, Error>;
}
