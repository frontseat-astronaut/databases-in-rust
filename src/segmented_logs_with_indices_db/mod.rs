use crate::kvdb::{error::Error, KVDb};
use std::fs::{self, read_dir};

use self::segment::{
    KVEntry::{Deleted, Present},
    Segment,
};

mod segment;

// TODO implement compaction
pub struct SegmentedLogsWithIndicesDb {
    dir_path: String,
    max_segment_records: u64,
    past_segments: Vec<Segment>,
    current_segment: Segment,
}

impl KVDb for SegmentedLogsWithIndicesDb {
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.create_new_segment_if_current_full()
            .and_then(|_| self.current_segment.set(key, value))
    }
    fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.create_new_segment_if_current_full()
            .and_then(|_| self.current_segment.delete(key))
    }
    fn get(&self, key: &str) -> Result<Option<String>, Error> {
        let mut result = Ok(None);
        // TODO: refactor this
        let mut check = |segment: &Segment| -> bool {
            match segment.get(key) {
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
                    false
                }
            }
        };
        if !check(&self.current_segment) {
            for segment in self.past_segments.iter().rev() {
                if check(&segment) {
                    break;
                }
            }
        }
        result
    }
}

impl SegmentedLogsWithIndicesDb {
    pub fn new(
        dir_path: &str,
        max_segment_records: u64,
    ) -> Result<SegmentedLogsWithIndicesDb, Error> {
        let mut segments = vec![];

        if let Err(e) = fs::create_dir_all(dir_path) {
            return Err(Error::from_io_error(&e));
        }

        match read_dir(dir_path) {
            Ok(contents) => {
                for dir_entry_result in contents {
                    match dir_entry_result {
                        Ok(dir_entry) => {
                            let path = dir_entry.path();
                            if !path.is_file() {
                                continue;
                            }
                            if let Some(stem) = path.file_stem() {
                                if let Some(stem_str) = stem.to_str() {
                                    if let Ok(segment_index) = stem_str.parse::<u64>() {
                                        match Segment::new(dir_path, segment_index) {
                                            Ok(segment) => segments.push(segment),
                                            Err(e) => return Err(e),
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => return Err(Error::from_io_error(&e)),
                    }
                }
            }
            Err(e) => return Err(Error::from_io_error(&e)),
        };

        segments.sort_by_key(|segment| segment.segment_index);

        let current_segment = match segments.pop() {
            Some(segment) => segment,
            None => match Segment::new(dir_path, 0) {
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
        self.current_segment.count_lines().and_then(|count| {
            if count >= self.max_segment_records {
                let segment_index = self.current_segment.segment_index + 1;
                self.past_segments.push(self.current_segment.clone());
                self.current_segment = match Segment::new(&self.dir_path, segment_index) {
                    Ok(segment) => segment,
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        })
    }
}
