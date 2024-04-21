use crate::{in_memory_db::InMemoryDb, kv_file::KVFile, kvdb::error::Error};
use KVEntry::{Deleted, Present};

#[derive(Clone)]
pub enum KVEntry<T: Clone> {
    Deleted,
    Present(T),
}

#[derive(Clone)]
pub struct Chunk {
    max_records: u64,
    file: KVFile,
    pub index: InMemoryDb<KVEntry<u64>>,
}

impl Chunk {
    pub fn new(dir_path: &str, file_name: &str, max_records: u64) -> Result<Chunk, Error> {
        let file = KVFile::new(dir_path, &file_name);
        let mut index = InMemoryDb::new();

        file.read_lines(&mut |key, maybe_value, offset| {
            match maybe_value {
                Some(_) => index.set(&key, &Present(offset)),
                None => index.set(&key, &Deleted),
            };
            Ok(())
        })
        .and(Ok(Chunk {
            max_records,
            file,
            index,
        }))
    }
    pub fn is_full(&self) -> Result<bool, Error> {
        self.file
            .count_lines()
            .and_then(|count| Ok(self.is_count_full(count)))
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
    pub fn add_entry(&mut self, key: &str, entry: &KVEntry<String>) -> Result<(), Error> {
        match entry {
            Present(value) => self.set(key, value),
            Deleted => self.delete(key),
        }
    }
    pub fn delete_file(self) -> Result<(), Error> {
        self.file.delete()
    }
    pub fn rename_file(&mut self, new_file_name: &str) -> Result<(), Error> {
        self.file.rename(new_file_name)
    }

    fn is_count_full(&self, count: u64) -> bool {
        count >= self.max_records
    }
}

#[derive(Clone)]
pub struct Segment {
    pub chunk: Chunk,
    pub id: usize,
}

impl Segment {
    pub fn new(dir_path: &str, id: usize, max_records: u64) -> Result<Segment, Error> {
        let file_name = Self::get_file_name(id);
        Chunk::new(dir_path, &file_name, max_records).map(|chunk| Segment { chunk, id })
    }
    pub fn from_chunk(mut chunk: Chunk, id: usize) -> Result<Segment, Error> {
        let file_name = Self::get_file_name(id);
        chunk.rename_file(&file_name).and(Ok(Segment { chunk, id }))
    }

    fn get_file_name(id: usize) -> String {
        format!("{}.txt", id)
    }
}
