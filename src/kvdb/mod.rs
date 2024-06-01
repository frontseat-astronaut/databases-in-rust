use crate::error::Error;

pub mod test;

pub trait KVDb {
    fn name(&self) -> String;
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error>;
    fn delete(&mut self, key: &str) -> Result<(), Error>;
    fn get(&self, key: &str) -> Result<Option<String>, Error>;
    fn add_entry(&mut self, key: &str, entry: &KVEntry<String>) -> Result<(), Error> {
        match entry {
            KVEntry::Deleted => self.delete(key),
            KVEntry::Present(value) => self.set(key, &value),
        }
    }
}

#[derive(Clone, Debug)]
pub enum KVEntry<T: Clone> {
    Deleted,
    Present(T),
}

impl<T: Clone> Into<Option<T>> for KVEntry<T> {
    fn into(self) -> Option<T> {
        match self {
            KVEntry::Deleted => None,
            KVEntry::Present(value) => Some(value),
        }
    }
}
