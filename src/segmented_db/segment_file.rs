use crate::{error::Error, kvdb::KVEntry};

pub trait SegmentFile {
    fn get_entry(&self, key: &str) -> Result<Option<KVEntry<String>>, Error>;
    fn should_replace(&self) -> Result<bool, Error>;

    fn add_entry(&mut self, key: &str, entry: &KVEntry<String>) -> Result<(), Error>;
    fn absorb(&mut self, other: &Self) -> Result<(), Error>;
    fn rename(&mut self, new_file_name: &str) -> Result<(), Error>;

    fn delete(self) -> Result<(), Error>;
}

pub trait SegmentFileFactory<T: SegmentFile> {
    fn new(&self, file_name: &str) -> Result<T, Error>;
    fn from_disk(&self, file_name: &str) -> Result<T, Error>;
}
