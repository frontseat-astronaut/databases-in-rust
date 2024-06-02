use crate::error::Error;

pub mod test;

pub trait KVDb {
    fn name(&self) -> String;
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error>;
    fn delete(&mut self, key: &str) -> Result<(), Error>;
    fn get(&self, key: &str) -> Result<Option<String>, Error>;
    fn set_status(&mut self, key: &str, status: &KeyStatus<String>) -> Result<(), Error> {
        match status {
            KeyStatus::Deleted => self.delete(key),
            KeyStatus::Present(value) => self.set(key, &value),
        }
    }
}

#[derive(Clone, Debug)]
pub enum KeyStatus<Value: Clone> {
    Deleted,
    Present(Value),
}

impl<T: Clone> Into<Option<T>> for KeyStatus<T> {
    fn into(self) -> Option<T> {
        match self {
            KeyStatus::Deleted => None,
            KeyStatus::Present(value) => Some(value),
        }
    }
}
