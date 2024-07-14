use crate::error::DbResult;

pub trait KVDb {
    fn description(&self) -> String;
    fn set(&mut self, key: &str, value: &str) -> DbResult<()>;
    fn delete(&mut self, key: &str) -> DbResult<()>;
    fn get(&mut self, key: &str) -> DbResult<Option<String>>;
    fn set_status(&mut self, key: &str, status: &KeyStatus<String>) -> DbResult<()> {
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
