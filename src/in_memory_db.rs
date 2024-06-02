use crate::{error::Error, kvdb::KVDb};
use std::collections::HashMap;

pub struct InMemoryDb<T: Clone> {
    map: HashMap<String, T>,
}

impl KVDb for InMemoryDb<String> {
    fn description(&self) -> String {
        "In-Memory DB".to_string()
    }
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        Ok(Self::set(self, key, &value.to_string()))
    }
    fn delete(&mut self, key: &str) -> Result<(), Error> {
        Ok(Self::delete(self, key))
    }
    fn get(&self, key: &str) -> Result<Option<String>, Error> {
        Ok(self.get(key))
    }
}

impl<T: Clone> InMemoryDb<T> {
    pub fn set(&mut self, key: &str, value: &T) -> () {
        self.map.insert(String::from(key), value.clone());
    }
    pub fn delete(&mut self, key: &str) -> () {
        self.map.remove(key);
    }
    pub fn get(&self, key: &str) -> Option<T> {
        self.map.get(key).map(|value| value.clone())
    }
    pub fn keys(&self) -> Vec<&String> {
        Vec::from_iter(self.map.keys())
    }
    pub fn new() -> InMemoryDb<T> {
        InMemoryDb {
            map: HashMap::new(),
        }
    }
}
