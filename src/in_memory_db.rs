use std::collections::HashMap;
use crate::kvdb::{self, KVDb};

pub struct InMemoryDb<T: Clone> {
    map: HashMap<String, T>,
}

impl KVDb for InMemoryDb<String> {
    fn set(&mut self, key: &str, value: &str) -> Result<(), kvdb::error::Error> {
        Self::set(self, key, &value.to_string())
    }
    fn delete(&mut self, key: &str) -> Result<(), kvdb::error::Error> {
        Self::delete(self, key)
    }
    fn get(&self, key: &str) -> Result<Option<String>, kvdb::error::Error> {
        Self::get(self, key)
    }
}

impl<T: Clone> InMemoryDb<T> {
    pub fn set(&mut self, key: &str, value: &T) -> Result<(), kvdb::error::Error> {
        self.map.insert(String::from(key), value.clone());
        Ok(())
    }
    pub fn delete(&mut self, key: &str) -> Result<(), kvdb::error::Error> {
        self.map.remove(key);
        Ok(())
    }
    pub fn get(&self, key: &str) -> Result<Option<T>, kvdb::error::Error> {
        Ok(self.map.get(key).map(|value| {
            value.clone()
        }))
    }
    pub fn new() -> InMemoryDb<T> {
        InMemoryDb{
            map: HashMap::new(),
        } 
    }
}

