use std::{
    fs::create_dir_all,
    mem::replace,
    sync::{Arc, RwLock},
    thread::{spawn, JoinHandle},
};

use crate::{
    check_kvdb_entry,
    error::Error,
    kvdb::KVEntry::{Deleted, Present},
    utils::{is_thread_running, process_dir_contents},
};

use self::segment::Segment;
use self::segment_file::{SegmentFile, SegmentFileFactory};

mod segment;
pub mod segment_file;

const TMP_SEGMENT_FILE_NAME: &str = "tmp.txt";

pub enum SegmentCreationPolicy {
    Triggered,
    Automatic,
}

pub struct SegmentedFilesDb<T, U>
where
    T: SegmentFile + Sync + Send + 'static,
    U: SegmentFileFactory<T> + Sync + Send + 'static,
{
    merging_threshold: u64,
    segment_creation_policy: SegmentCreationPolicy,
    locked_past_segments: Arc<RwLock<Vec<Segment<T>>>>,
    locked_current_segment: Arc<RwLock<Segment<T>>>,
    file_factory: Arc<U>,
    join_handle: Option<JoinHandle<Result<(), Error>>>,
}

impl<T, U> SegmentedFilesDb<T, U>
where
    T: SegmentFile + Sync + Send + 'static,
    U: SegmentFileFactory<T> + Sync + Send + 'static,
{
    pub fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.maybe_create_fresh_segment()?;
        self.locked_current_segment
            .write()
            .map_err(Error::from)
            .and_then(|mut current_segment| {
                current_segment
                    .file
                    .add_entry(key, &Present(value.to_owned()))
            })
    }
    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.maybe_create_fresh_segment()?;
        self.locked_current_segment
            .write()
            .map_err(Error::from)
            .and_then(|mut current_segment| current_segment.file.add_entry(key, &Deleted))
    }
    pub fn get(&self, key: &str) -> Result<Option<String>, Error> {
        {
            let current_segment = self.locked_current_segment.read()?;
            check_kvdb_entry!(current_segment.file.get_entry(key)?);
        }

        {
            let past_segments = self.locked_past_segments.read()?;
            for segment in past_segments.iter().rev() {
                check_kvdb_entry!(segment.file.get_entry(key)?);
            }
        }

        Ok(None)
    }
    pub fn create_fresh_segment(&mut self) -> Result<(), Error> {
        if self
            .locked_current_segment
            .read()?
            .file
            .ready_to_be_archived()?
        {
            let past_segments_len;
            {
                let mut past_segments = self.locked_past_segments.write()?;
                let mut current_segment = self.locked_current_segment.write()?;

                let latest_past_segment_id: usize = past_segments
                    .last()
                    .map(|segment| segment.id + 1)
                    .get_or_insert(0)
                    .clone();
                current_segment
                    .change_id(latest_past_segment_id)
                    .map_err(|e| Error::wrap("error in changing id of current segment", e))?;

                let new_segment_id = latest_past_segment_id + 1;
                let new_segment = Segment::new(new_segment_id, &(*self.file_factory))?;
                let latest_past_segment = replace(&mut (*current_segment), new_segment);

                past_segments.push(latest_past_segment);
                past_segments_len = past_segments.len();
            }

            if u64::try_from(past_segments_len).unwrap() > self.merging_threshold {
                self.maybe_merge_past_segments_in_background();
            }
        }
        Ok(())
    }
}

impl<T, U> Drop for SegmentedFilesDb<T, U>
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

impl<T, U> SegmentedFilesDb<T, U>
where
    T: SegmentFile + Sync + Send + 'static,
    U: SegmentFileFactory<T> + Sync + Send + 'static,
{
    pub fn new(
        dir_path: &str,
        merging_threshold: u64,
        segment_creation_policy: SegmentCreationPolicy,
        file_factory: U,
    ) -> Result<Self, Error> {
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

        Ok(SegmentedFilesDb {
            merging_threshold,
            segment_creation_policy,
            locked_past_segments: Arc::new(RwLock::new(segments)),
            locked_current_segment: Arc::new(RwLock::new(current_segment)),
            file_factory: Arc::new(file_factory),
            join_handle: None,
        })
    }
    fn maybe_create_fresh_segment(&mut self) -> Result<(), Error> {
        if is_thread_running(&self.join_handle) {
            return Ok(());
        }
        if let SegmentCreationPolicy::Automatic = self.segment_creation_policy {
            self.create_fresh_segment()?
        }
        Ok(())
    }
    fn maybe_merge_past_segments_in_background(&mut self) {
        if is_thread_running(&self.join_handle) {
            return;
        }

        let locked_past_segments = Arc::clone(&self.locked_past_segments);
        let file_factory = Arc::clone(&self.file_factory);
        self.join_handle = Some(spawn(move || -> Result<(), Error> {
            let mut merged_segment_file = file_factory.new(TMP_SEGMENT_FILE_NAME)?;

            for segment in locked_past_segments.read()?.iter().rev() {
                merged_segment_file.absorb(&segment.file)?;
            }

            {
                let mut past_segments = locked_past_segments.write()?;
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
