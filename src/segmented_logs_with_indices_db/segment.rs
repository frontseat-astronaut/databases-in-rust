use crate::{in_memory_db::InMemoryDb, kv_file::KVFile, kvdb::error::Error};
use KVEntry::{Deleted, Present};

#[derive(Clone)]
pub enum KVEntry<T: Clone> {
    Deleted,
    Present(T),
}

#[derive(Clone)]
pub struct Segment {
    max_records: u64,
    file: KVFile,
    index: InMemoryDb<KVEntry<u64>>,
    pub segment_index: u64,
}

impl Segment {
    pub fn new(dir_path: &str, segment_index: u64, max_records: u64) -> Result<Segment, Error> {
        let file_name = Self::get_file_name(segment_index);
        let file = KVFile::new(dir_path, &file_name);
        let mut index = InMemoryDb::new();

        file.read_lines(&mut |key, maybe_value, offset| {
            match maybe_value {
                Some(_) => index.set(&key, &Present(offset)),
                None => index.set(&key, &Deleted),
            };
            Ok(())
        })
        .and(Ok(Segment {
            max_records,
            file,
            index,
            segment_index,
        }))
    }
    pub fn is_full(&self) -> Result<bool, Error> {
        self.file
            .count_lines()
            .and_then(|count| Ok(count >= self.max_records))
    }
    pub fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.file
            .append_line(key, Some(value))
            .and_then(|offset| Ok(self.index.set(key, &Present(offset))))
    }
    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.file
            .append_line(key, None)
            .and_then(|_| Ok(self.index.set(key, &Deleted)))
    }
    pub fn get(&self, key: &str) -> Result<Option<KVEntry<String>>, Error> {
        match self.index.get(key) {
            Some(Present(offset)) => self
                .file
                .get_at_offset(offset)
                .and_then(|maybe_value| Ok(maybe_value.and_then(|value| Some(Present(value))))),
            Some(Deleted) => Ok(Some(Deleted)),
            None => Ok(None),
        }
    }

    fn get_file_name(index: u64) -> String {
        format!("{}.txt", index)
    }
}
