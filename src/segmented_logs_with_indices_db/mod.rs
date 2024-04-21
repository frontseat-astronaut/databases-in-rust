use crate::kvdb::{error::Error, KVDb};
use std::{
    collections::HashSet,
    fs::{self, read_dir, DirEntry},
};

use self::segment::{
    Chunk,
    KVEntry::{Deleted, Present},
    Segment,
};

mod segment;

pub struct SegmentedLogsWithIndicesDb {
    dir_path: String,
    max_segment_records: u64,
    past_segments: Vec<Segment>,
    current_segment: Segment,
}

impl KVDb for SegmentedLogsWithIndicesDb {
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.create_new_segment_if_current_full()
            .and_then(|_| self.current_segment.chunk.set(key, value))
    }
    fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.create_new_segment_if_current_full()
            .and_then(|_| self.current_segment.chunk.delete(key))
    }
    fn get(&self, key: &str) -> Result<Option<String>, Error> {
        let mut result = Ok(None);
        let mut check_segment = |segment: &Segment| match segment.chunk.get(key) {
            Ok(None) => false,
            Ok(Some(Deleted)) => {
                result = Ok(None);
                true
            }
            Ok(Some(Present(value))) => {
                result = Ok(Some(value));
                true
            }
            Err(e) => {
                result = Err(e);
                true
            }
        };
        if !check_segment(&self.current_segment) {
            for segment in self.past_segments.iter().rev() {
                if check_segment(&segment) {
                    break;
                }
            }
        }
        result
    }
}

// TODO: do error handling
impl SegmentedLogsWithIndicesDb {
    pub fn new(
        dir_path: &str,
        max_segment_records: u64,
    ) -> Result<SegmentedLogsWithIndicesDb, Error> {
        if let Err(e) = fs::create_dir_all(dir_path) {
            return Err(Error::from_io_error(&e));
        }

        let mut segments = vec![];
        let mut process_dir_entry = |dir_entry: &DirEntry| {
            let path = dir_entry.path();
            if path.is_file() {
                if let Some(stem) = path.file_stem() {
                    if let Some(stem_str) = stem.to_str() {
                        if let Ok(id) = stem_str.parse::<usize>() {
                            match Segment::new(dir_path, id, max_segment_records) {
                                Ok(segment) => segments.push(segment),
                                Err(e) => return Err(e),
                            };
                        }
                    }
                }
            }
            Ok(())
        };
        match read_dir(dir_path) {
            Ok(contents) => {
                for dir_entry_result in contents {
                    match dir_entry_result {
                        Ok(dir_entry) => {
                            if let Err(e) = process_dir_entry(&dir_entry) {
                                return Err(e);
                            }
                        }
                        Err(e) => return Err(Error::from_io_error(&e)),
                    }
                }
            }
            Err(e) => return Err(Error::from_io_error(&e)),
        };

        segments.sort_by_key(|segment| segment.id);

        let current_segment = match segments.pop() {
            Some(segment) => segment,
            None => match Segment::new(dir_path, 0, max_segment_records) {
                Ok(segment) => segment,
                Err(e) => return Err(e),
            },
        };

        Ok(SegmentedLogsWithIndicesDb {
            dir_path: dir_path.to_string(),
            max_segment_records,
            past_segments: segments,
            current_segment,
        })
    }

    fn create_new_segment_if_current_full(&mut self) -> Result<(), Error> {
        self.current_segment.chunk.is_full().and_then(|is_full| {
            if is_full {
                let id = self.current_segment.id + 1;
                self.past_segments.push(self.current_segment.clone());
                self.current_segment =
                    match Segment::new(&self.dir_path, id, self.max_segment_records) {
                        Ok(segment) => segment,
                        Err(e) => return Err(e),
                    };
            }

            if let Err(e) = self.do_compaction() {
                return Err(e);
            }

            Ok(())
        })
    }

    fn do_compaction(&mut self) -> Result<(), Error> {
        let mut tmp_chunks = vec![];
        let add_fresh_chunk = |tmp_chunks: &mut Vec<Chunk>| {
            let fresh_chunk = Chunk::new(
                &self.dir_path,
                &format!("tmp{}.txt", tmp_chunks.len()),
                self.max_segment_records,
            )
            .unwrap();
            tmp_chunks.push(fresh_chunk);
        };
        add_fresh_chunk(&mut tmp_chunks);

        let mut keys_set = HashSet::new();
        for segment in self.past_segments.iter().rev() {
            for key in segment.chunk.index.keys() {
                if !keys_set.contains(key) {
                    keys_set.insert(key.to_string());

                    match segment.chunk.get(key) {
                        Ok(Some(entry)) => {
                            let current_chunk = tmp_chunks.last_mut().unwrap();
                            if let Err(e) = current_chunk.add_entry(key, &entry) {
                                return Err(e);
                            }
                            if current_chunk.is_full().unwrap() {
                                add_fresh_chunk(&mut tmp_chunks);
                            }
                        }
                        Ok(None) => {}
                        Err(e) => return Err(e),
                    }
                }
            }
        }

        while !self.past_segments.is_empty() {
            let segment = self.past_segments.pop().unwrap();
            segment.chunk.delete_file().unwrap();
        }

        let mut segment_id = 0;
        while !tmp_chunks.is_empty() {
            let tmp_chunk = tmp_chunks.pop().unwrap();
            let segment = Segment::from_chunk(tmp_chunk, segment_id).unwrap();
            self.past_segments.push(segment);
            segment_id += 1;
        }

        Ok(())
    }
}
