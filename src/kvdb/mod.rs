use error::Error;

pub mod error;
pub mod test;

pub trait KVDb {
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error>;
    fn delete(&mut self, key: &str) -> Result<(), Error>;
    fn get(&self, key: &str) -> Result<Option<String>, Error>;
}

#[derive(Clone)]
pub enum KVEntry<T: Clone> {
    Deleted,
    Present(T),
}
