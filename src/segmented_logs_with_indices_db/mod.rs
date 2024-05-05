use crate::{
    check_kvdb_result,
    kvdb::{
        error::Error,
        KVDb,
        KVEntry::{Deleted, Present},
    },
    utils::process_dir_contents,
};
use std::{
    collections::HashSet,
    fs::{create_dir_all, remove_file},
    mem::replace,
    path::PathBuf,
    sync::{Arc, RwLock},
    thread::{spawn, JoinHandle},
};

use self::segment::{Chunk, Segment};

mod segment;

pub struct SegmentedLogsWithIndicesDb {
    dir_path: String,
    segment_size_threshold: u64,
    merging_threshold: u64,
    past_segments_locked: Arc<RwLock<Vec<Segment>>>,
    current_segment_locked: Arc<RwLock<Segment>>,
    merging_thread_join_handle: Option<JoinHandle<()>>,
}

impl KVDb for SegmentedLogsWithIndicesDb {
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.create_new_segment_if_current_full().and_then(|_| {
            self.current_segment_locked
                .write()
                .map_err(Error::from)
                .and_then(|mut current_segment| current_segment.chunk.set(key, value))
        })
    }
    fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.create_new_segment_if_current_full().and_then(|_| {
            self.current_segment_locked
                .write()
                .map_err(Error::from)
                .and_then(|mut current_segment| current_segment.chunk.delete(key))
        })
    }
    fn get(&self, key: &str) -> Result<Option<String>, Error> {
        {
            let current_segment = self.current_segment_locked.read()?;
            check_kvdb_result!(current_segment.chunk.get(key));
        }

        {
            let past_segments = self.past_segments_locked.read()?;
            for segment in past_segments.iter().rev() {
                check_kvdb_result!(segment.chunk.get(key));
            }
        }

        Ok(None)
    }
}

impl Drop for SegmentedLogsWithIndicesDb {
    fn drop(&mut self) {
        if let Some(handle) = self.merging_thread_join_handle.take() {
            let _ = handle.join();
        }
    }
}

impl SegmentedLogsWithIndicesDb {
    pub fn new(
        dir_path: &str,
        segment_size_threshold: u64,
        merging_threshold: u64,
    ) -> Result<SegmentedLogsWithIndicesDb, Error> {
        create_dir_all(dir_path)?;

        let mut segments = vec![];
        process_dir_contents(dir_path, &mut |path: PathBuf| {
            if path.is_file() {
                if let Some(stem) = path.file_stem() {
                    if let Some(stem_str) = stem.to_str() {
                        if let Ok(id) = stem_str.parse::<usize>() {
                            let segment = Segment::new(dir_path, id)?;
                            segments.push(segment);
                        }
                    }
                }
            }
            Ok(())
        })?;

        segments.sort_by_key(|segment| segment.id);

        let current_segment = match segments.pop() {
            Some(segment) => segment,
            None => Segment::new(dir_path, 0)?,
        };

        Ok(SegmentedLogsWithIndicesDb {
            dir_path: dir_path.to_string(),
            segment_size_threshold,
            merging_threshold,
            past_segments_locked: Arc::new(RwLock::new(segments)),
            current_segment_locked: Arc::new(RwLock::new(current_segment)),
            merging_thread_join_handle: None,
        })
    }

    fn create_new_segment_if_current_full(&mut self) -> Result<(), Error> {
        if self.is_merging_running() {
            return Ok(());
        }

        let should_merge;
        {
            let mut past_segments = self.past_segments_locked.write()?;
            let mut current_segment = self.current_segment_locked.write()?;
            if Self::is_chunk_full(&current_segment.chunk, self.segment_size_threshold)? {
                let id = current_segment.id + 1;
                let past_segment =
                    replace(&mut (*current_segment), Segment::new(&self.dir_path, id)?);
                past_segments.push(past_segment);
            }

            should_merge = u64::try_from(past_segments.len()).unwrap() > self.merging_threshold;
        }

        if should_merge {
            self.run_merging_in_background();
        }
        Ok(())
    }

    fn run_merging_in_background(&mut self) {
        if self.is_merging_running() {
            return;
        }

        let past_segments = Arc::clone(&self.past_segments_locked);
        let current_segment = Arc::clone(&self.current_segment_locked);
        let dir_path = self.dir_path.clone();
        let segment_size_threshold = self.segment_size_threshold;

        self.merging_thread_join_handle = Some(spawn(move || {
            println!("merging thread started");
            Self::do_merging(
                past_segments,
                current_segment,
                dir_path,
                segment_size_threshold,
            )
            .unwrap();
        }));
    }

    fn is_merging_running(&self) -> bool {
        if let Some(join_handle) = &self.merging_thread_join_handle {
            if !join_handle.is_finished() {
                return true;
            }
        }
        false
    }

    // TODO: handle error scenarios better
    fn do_merging(
        past_segments_locked: Arc<RwLock<Vec<Segment>>>,
        current_segment_locked: Arc<RwLock<Segment>>,
        dir_path: String,
        segment_size_threshold: u64,
    ) -> Result<(), Error> {
        Self::delete_tmp_files(&dir_path)?;

        let mut tmp_chunks = vec![];
        let add_fresh_chunk = |tmp_chunks: &mut Vec<Chunk>| {
            Chunk::new(&dir_path, &format!("tmp{}.txt", tmp_chunks.len())).and_then(|fresh_chunk| {
                tmp_chunks.push(fresh_chunk);
                Ok(())
            })
        };
        add_fresh_chunk(&mut tmp_chunks)?;

        {
            let past_segments = past_segments_locked.read()?;
            if past_segments.is_empty() {
                return Ok(());
            }
            let mut keys_set = HashSet::new();
            for segment in past_segments.iter().rev() {
                for key in segment.chunk.index.keys() {
                    if !keys_set.contains(key) {
                        keys_set.insert(key.to_string());

                        let maybe_entry = segment.chunk.get(key)?;
                        if let Some(entry) = maybe_entry {
                            let current_chunk = tmp_chunks.last_mut().unwrap();
                            current_chunk.add_entry(key, &entry)?;
                            if Self::is_chunk_full(&current_chunk, segment_size_threshold)? {
                                add_fresh_chunk(&mut tmp_chunks)?
                            }
                        }
                    }
                }
            }
        };

        let mut current_segment_id = 0;
        {
            let mut past_segments = past_segments_locked.write()?;
            while !past_segments.is_empty() {
                let segment = past_segments.pop().unwrap();
                segment.chunk.delete_file()?;
            }
            while !tmp_chunks.is_empty() {
                let tmp_chunk = tmp_chunks.pop().unwrap();
                let segment = Segment::from_chunk(tmp_chunk, current_segment_id)?;
                past_segments.push(segment);
                current_segment_id += 1;
            }
        }

        let mut current_segment = current_segment_locked.write()?;
        current_segment.change_id(current_segment_id)
    }

    fn delete_tmp_files(dir_path: &str) -> Result<(), Error> {
        process_dir_contents(&dir_path, &mut |path: PathBuf| {
            if path.is_file() {
                if let Some(file_name_os_str) = path.file_name() {
                    if let Some(file_name) = file_name_os_str.to_str() {
                        if file_name.starts_with("tmp") {
                            remove_file(path)?;
                        }
                    }
                }
            }
            Ok(())
        })
    }

    fn is_chunk_full(chunk: &Chunk, segment_size_threshold: u64) -> Result<bool, Error> {
        chunk
            .size()
            .and_then(|size| Ok(size >= segment_size_threshold))
    }
}
