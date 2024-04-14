use error::Error;

pub mod error;
pub mod test;

pub trait KVDb {
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error>;
    fn get(&self, key: &str) -> Result<Option<String>, Error>;
}