use std::{
    fs::create_dir_all,
    mem::replace,
    sync::{Arc, RwLock},
    thread::{spawn, JoinHandle},
};

use crate::{
    check_kvdb_entry,
    error::Error,
    kvdb::{
        KVDb,
        KVEntry::{Deleted, Present},
    },
    utils::{is_thread_running, process_dir_contents},
};

use self::segment::Segment;
use self::segment_file::{SegmentFile, SegmentFileFactory};

mod segment;
pub mod segment_file;

const TMP_SEGMENT_FILE_NAME: &str = "tmp.txt";

pub struct SegmentedDb<T, U>
where
    T: SegmentFile + Sync + Send + 'static,
    U: SegmentFileFactory<T> + Sync + Send + 'static,
{
    merging_threshold: u64,
    past_segments_lock: Arc<RwLock<Vec<Segment<T>>>>,
    current_segment_lock: Arc<RwLock<Segment<T>>>,
    file_factory: Arc<U>,
    join_handle: Option<JoinHandle<Result<(), Error>>>,
}

impl<T, U> KVDb for SegmentedDb<T, U>
where
    T: SegmentFile + Sync + Send + 'static,
    U: SegmentFileFactory<T> + Sync + Send + 'static,
{
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.maybe_create_new_segment().and_then(|_| {
            self.current_segment_lock
                .write()
                .map_err(Error::from)
                .and_then(|mut current_segment| {
                    current_segment
                        .file
                        .add_entry(key, &Present(value.to_owned()))
                })
        })
    }
    fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.maybe_create_new_segment().and_then(|_| {
            self.current_segment_lock
                .write()
                .map_err(Error::from)
                .and_then(|mut current_segment| current_segment.file.add_entry(key, &Deleted))
        })
    }
    fn get(&self, key: &str) -> Result<Option<String>, Error> {
        {
            let current_segment = self.current_segment_lock.read()?;
            check_kvdb_entry!(current_segment.file.get_entry(key)?);
        }

        {
            let past_segments = self.past_segments_lock.read()?;
            for segment in past_segments.iter().rev() {
                check_kvdb_entry!(segment.file.get_entry(key)?);
            }
        }

        Ok(None)
    }
}

impl<T, U> Drop for SegmentedDb<T, U>
where
    T: SegmentFile + Sync + Send + 'static,
    U: SegmentFileFactory<T> + Sync + Send + 'static,
{
    fn drop(&mut self) {
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
    }
}

impl<T, U> SegmentedDb<T, U>
where
    T: SegmentFile + Sync + Send + 'static,
    U: SegmentFileFactory<T> + Sync + Send + 'static,
{
    pub fn new(dir_path: &str, merging_threshold: u64, file_factory: U) -> Result<Self, Error> {
        create_dir_all(dir_path)?;

        let mut segments = vec![];
        process_dir_contents(dir_path, &mut |path| {
            if let Some(segment) = Segment::try_from_disk(&path, &file_factory)? {
                segments.push(segment);
            }
            Ok(())
        })?;
        segments.sort_by_key(|segment| segment.id);

        let current_segment = match segments.pop() {
            Some(segment) => segment,
            None => Segment::new(0, &file_factory)?,
        };

        Ok(SegmentedDb {
            merging_threshold,
            past_segments_lock: Arc::new(RwLock::new(segments)),
            current_segment_lock: Arc::new(RwLock::new(current_segment)),
            file_factory: Arc::new(file_factory),
            join_handle: None,
        })
    }
    fn maybe_create_new_segment(&mut self) -> Result<(), Error> {
        if is_thread_running(&self.join_handle) {
            return Ok(());
        }

        if self.current_segment_lock.read()?.file.should_replace()? {
            let past_segments_len;
            {
                let mut past_segments = self.past_segments_lock.write()?;
                let mut current_segment = self.current_segment_lock.write()?;

                let latest_past_segment_id: usize = past_segments
                    .last()
                    .map(|segment| segment.id + 1)
                    .get_or_insert(0)
                    .clone();
                current_segment.change_id(latest_past_segment_id)?;

                let new_segment_id = latest_past_segment_id + 1;
                let new_segment = Segment::new(new_segment_id, &(*self.file_factory))?;
                let latest_past_segment = replace(&mut (*current_segment), new_segment);

                past_segments.push(latest_past_segment);
                past_segments_len = past_segments.len();
            }

            if u64::try_from(past_segments_len).unwrap() >= self.merging_threshold {
                self.merge_past_segments_in_background();
            }
        }
        Ok(())
    }
    fn merge_past_segments_in_background(&mut self) {
        if is_thread_running(&self.join_handle) {
            return;
        }

        let past_segments_lock = Arc::clone(&self.past_segments_lock);
        let file_factory = Arc::clone(&self.file_factory);
        self.join_handle = Some(spawn(move || -> Result<(), Error> {
            let mut merged_segment_file = file_factory.new(TMP_SEGMENT_FILE_NAME)?;

            for segment in past_segments_lock.read()?.iter().rev() {
                merged_segment_file.absorb(&segment.file)?;
            }

            {
                let mut past_segments = past_segments_lock.write()?;
                while !past_segments.is_empty() {
                    let past_segment = past_segments.pop().unwrap();
                    past_segment.file.delete()?;
                }

                past_segments.push(Segment::from_file(merged_segment_file, 0)?)
            }

            Ok(())
        }));
    }
}
