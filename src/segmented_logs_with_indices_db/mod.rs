use crate::{
    check_kvdb_result,
    kvdb::{
        error::Error,
        KVDb,
        KVEntry::{Deleted, Present},
    },
    unwrap_or_return, unwrap_or_return_io_error,
    utils::{process_dir_contents, read_locked, write_locked},
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
    past_segments: Arc<RwLock<Vec<Segment>>>,
    current_segment: Arc<RwLock<Segment>>,
    merging_thread_join_handle: Option<JoinHandle<()>>,
}

impl KVDb for SegmentedLogsWithIndicesDb {
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.create_new_segment_if_current_full().and_then(|_| {
            write_locked(&self.current_segment)
                .and_then(|mut current_segment| current_segment.chunk.set(key, value))
        })
    }
    fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.create_new_segment_if_current_full().and_then(|_| {
            write_locked(&self.current_segment)
                .and_then(|mut current_segment| current_segment.chunk.delete(key))
        })
    }
    fn get(&self, key: &str) -> Result<Option<String>, Error> {
        let current_segments = unwrap_or_return!(read_locked(&self.current_segment));
        check_kvdb_result!(current_segments.chunk.get(key));
        drop(current_segments);

        let past_segments = unwrap_or_return!(read_locked(&self.past_segments));
        for segment in past_segments.iter().rev() {
            check_kvdb_result!(segment.chunk.get(key));
        }
        drop(past_segments);

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
        unwrap_or_return_io_error!(create_dir_all(dir_path));

        let mut segments = vec![];
        unwrap_or_return!(process_dir_contents(dir_path, &mut |path: PathBuf| {
            if path.is_file() {
                if let Some(stem) = path.file_stem() {
                    if let Some(stem_str) = stem.to_str() {
                        if let Ok(id) = stem_str.parse::<usize>() {
                            let segment = unwrap_or_return!(Segment::new(dir_path, id));
                            segments.push(segment);
                        }
                    }
                }
            }
            Ok(())
        }));

        segments.sort_by_key(|segment| segment.id);

        let current_segment = match segments.pop() {
            Some(segment) => segment,
            None => unwrap_or_return!(Segment::new(dir_path, 0,)),
        };

        Ok(SegmentedLogsWithIndicesDb {
            dir_path: dir_path.to_string(),
            segment_size_threshold,
            merging_threshold,
            past_segments: Arc::new(RwLock::new(segments)),
            current_segment: Arc::new(RwLock::new(current_segment)),
            merging_thread_join_handle: None,
        })
    }

    fn create_new_segment_if_current_full(&mut self) -> Result<(), Error> {
        if self.is_merging_running() {
            return Ok(());
        }
        write_locked(&self.past_segments)
            .and_then(|mut past_segments| {
                write_locked(&self.current_segment).and_then(|mut current_segment| {
                    if unwrap_or_return!(Self::is_chunk_full(
                        &current_segment.chunk,
                        self.segment_size_threshold
                    )) {
                        let id = current_segment.id + 1;
                        let past_segment = replace(
                            &mut (*current_segment),
                            unwrap_or_return!(Segment::new(&self.dir_path, id)),
                        );
                        past_segments.push(past_segment);
                    }
                    Ok(u64::try_from(past_segments.len()).unwrap() > self.merging_threshold)
                })
            })
            .and_then(|should_merge| {
                if should_merge {
                    self.run_merging_in_background();
                }
                Ok(())
            })
    }

    fn run_merging_in_background(&mut self) {
        if self.is_merging_running() {
            return;
        }

        let past_segments = Arc::clone(&self.past_segments);
        let current_segment = Arc::clone(&self.current_segment);
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
        past_segments: Arc<RwLock<Vec<Segment>>>,
        current_segment: Arc<RwLock<Segment>>,
        dir_path: String,
        segment_size_threshold: u64,
    ) -> Result<(), Error> {
        unwrap_or_return!(Self::delete_tmp_files(&dir_path));

        let mut tmp_chunks = vec![];
        let add_fresh_chunk = |tmp_chunks: &mut Vec<Chunk>| {
            Chunk::new(&dir_path, &format!("tmp{}.txt", tmp_chunks.len())).and_then(|fresh_chunk| {
                tmp_chunks.push(fresh_chunk);
                Ok(())
            })
        };
        unwrap_or_return!(add_fresh_chunk(&mut tmp_chunks));

        match read_locked(&past_segments) {
            Ok(past_segments) => {
                if past_segments.is_empty() {
                    return Ok(());
                }

                let mut keys_set = HashSet::new();
                for segment in past_segments.iter().rev() {
                    for key in segment.chunk.index.keys() {
                        if !keys_set.contains(key) {
                            keys_set.insert(key.to_string());

                            match segment.chunk.get(key) {
                                Ok(Some(entry)) => {
                                    let current_chunk = tmp_chunks.last_mut().unwrap();
                                    unwrap_or_return!(current_chunk.add_entry(key, &entry));
                                    if unwrap_or_return!(Self::is_chunk_full(
                                        &current_chunk,
                                        segment_size_threshold
                                    )) {
                                        unwrap_or_return!(add_fresh_chunk(&mut tmp_chunks))
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => return Err(e),
                            }
                        }
                    }
                }
            }
            Err(e) => return Err(e),
        }

        write_locked(&past_segments)
            .and_then(|mut past_segments| {
                while !past_segments.is_empty() {
                    let segment = past_segments.pop().unwrap();
                    unwrap_or_return!(segment.chunk.delete_file());
                }

                let mut segment_id = 0;
                while !tmp_chunks.is_empty() {
                    let tmp_chunk = tmp_chunks.pop().unwrap();
                    let segment = unwrap_or_return!(Segment::from_chunk(tmp_chunk, segment_id));
                    past_segments.push(segment);
                    segment_id += 1;
                }

                Ok(segment_id)
            })
            .and_then(|new_current_segment_id| {
                write_locked(&current_segment).and_then(|mut current_segment| {
                    current_segment.change_id(new_current_segment_id)
                })
            })
    }

    fn delete_tmp_files(dir_path: &str) -> Result<(), Error> {
        process_dir_contents(&dir_path, &mut |path: PathBuf| {
            if path.is_file() {
                if let Some(file_name_os_str) = path.file_name() {
                    if let Some(file_name) = file_name_os_str.to_str() {
                        if file_name.starts_with("tmp") {
                            unwrap_or_return_io_error!(remove_file(path));
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
