use crate::{
    error::Error,
    in_memory_db::InMemoryDb,
    kv_file::KVFile,
    kvdb::KVEntry,
    segmented_files_db::segment_file::{SegmentFile, SegmentFileFactory},
};
use KVEntry::{Deleted, Present};

pub struct File {
    kvfile: KVFile,
    index: InMemoryDb<KVEntry<u64>>,
    file_size_threshold: u64,
}

impl SegmentFile for File {
    fn get_entry(&self, key: &str) -> Result<Option<KVEntry<String>>, Error> {
        match self.index.get(key) {
            Some(Present(offset)) => self
                .kvfile
                .get_at_offset(offset)
                .and_then(|maybe_value| Ok(maybe_value.and_then(|value| Some(Present(value))))),
            Some(Deleted) => Ok(Some(Deleted)),
            None => Ok(None),
        }
    }
    fn ready_to_be_archived(&self) -> Result<bool, Error> {
        Ok(self.kvfile.size()? > self.file_size_threshold)
    }
    fn add_entry(&mut self, key: &str, entry: &KVEntry<String>) -> Result<(), Error> {
        self.kvfile.append_line(key, &entry).and_then(|offset| {
            Ok(self.index.set(
                key,
                &match entry {
                    Present(_) => Present(offset),
                    Deleted => Deleted,
                },
            ))
        })
    }
    fn absorb(&mut self, other: &Self) -> Result<(), Error> {
        for key in other.index.keys() {
            if self.index.get(key).is_none() {
                self.add_entry(key.as_str(), &other.get_entry(key)?.unwrap())?;
            }
        }
        Ok(())
    }
    fn rename(&mut self, new_file_name: &str) -> Result<(), Error> {
        self.kvfile.rename(new_file_name)
    }
    fn delete(self) -> Result<(), Error> {
        self.kvfile.delete()
    }
}

pub struct Factory {
    pub dir_path: String,
    pub file_size_threshold: u64,
}

impl SegmentFileFactory<File> for Factory {
    fn new(&self, file_name: &str) -> Result<File, Error> {
        let kvfile = KVFile::new(&self.dir_path, file_name);
        let index = InMemoryDb::new();
        Ok(File {
            kvfile,
            index,
            file_size_threshold: self.file_size_threshold,
        })
    }
    fn from_disk(&self, file_name: &str) -> Result<File, Error> {
        let kvfile = KVFile::new(&self.dir_path, file_name);
        let mut index = InMemoryDb::new();
        for line_result in kvfile.iter()? {
            let line = line_result?;
            match line.entry {
                Present(_) => index.set(&line.key, &Present(line.offset)),
                Deleted => index.set(&line.key, &Deleted),
            }
        }
        Ok(File {
            kvfile,
            index,
            file_size_threshold: self.file_size_threshold,
        })
    }
}
