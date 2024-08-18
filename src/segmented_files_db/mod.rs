use segment_file::SegmentReaderFactory;
use std::{
    fs::create_dir_all,
    mem::replace,
    sync::{Arc, RwLock},
    thread::{spawn, JoinHandle},
};

use self::segment::Segment;
use self::segment_file::{SegmentFile, SegmentFileFactory};
use crate::error::DbResult;
use crate::tmp_file_names::TMP_SEGMENT_FILE_NAME;
use crate::{
    check_key_status,
    error::Error,
    kvdb::KeyStatus::{Deleted, Present},
    utils::{is_thread_running, process_dir_contents},
};

mod segment;
pub mod segment_file;

pub enum SegmentCreationPolicy {
    Triggered,
    Automatic,
}

pub struct SegmentedFilesDb<F, U, V>
where
    F: SegmentFile + Sync + Send + 'static,
    U: SegmentFileFactory<F> + Sync + Send + 'static,
    V: SegmentReaderFactory<F> + Sync + Send + 'static,
{
    merging_threshold: u64,
    segment_creation_policy: SegmentCreationPolicy,
    locked_past_segments: Arc<RwLock<Vec<Segment<F>>>>,
    current_segment: Segment<F>,
    file_factory: Arc<U>,
    reader_factory: Arc<V>,
    merging_thread_join_handle: Option<JoinHandle<()>>,
}

impl<F, U, V> SegmentedFilesDb<F, U, V>
where
    F: SegmentFile + Sync + Send + 'static,
    U: SegmentFileFactory<F> + Sync + Send + 'static,
    V: SegmentReaderFactory<F> + Sync + Send + 'static,
{
    pub fn set(&mut self, key: &str, value: &str) -> DbResult<()> {
        self.maybe_create_fresh_segment()?;
        self.current_segment
            .locked_file
            .write()?
            .set_status(key, &Present(value.to_owned()))
    }
    pub fn delete(&mut self, key: &str) -> DbResult<()> {
        self.maybe_create_fresh_segment()?;
        self.current_segment
            .locked_file
            .write()?
            .set_status(key, &Deleted)
    }
    pub fn get(&mut self, key: &str) -> DbResult<Option<String>> {
        {
            check_key_status!(self.current_segment.locked_file.write()?.get_status(key)?);
        }

        {
            let past_segments = self.locked_past_segments.read()?;
            for segment in past_segments.iter().rev() {
                check_key_status!(segment.locked_file.write()?.get_status(key)?);
            }
        }

        Ok(None)
    }
    pub fn create_fresh_segment(&mut self) -> DbResult<()> {
        let should_do = self
            .current_segment
            .locked_file
            .write()?
            .ready_to_be_archived()?;
        if should_do {
            let past_segments_len;
            {
                let mut past_segments = self.locked_past_segments.write()?;

                let latest_past_segment_id: usize = past_segments
                    .last()
                    .map(|segment| segment.id + 1)
                    .get_or_insert(0)
                    .clone();
                self.current_segment
                    .change_id(latest_past_segment_id)
                    .map_err(|e| Error::wrap("error in changing id of current segment", e))?;

                let new_segment_id = latest_past_segment_id + 1;
                let new_segment = Segment::new(new_segment_id, &(*self.file_factory))?;
                let latest_past_segment = replace(&mut self.current_segment, new_segment);

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

impl<F, U, V> Drop for SegmentedFilesDb<F, U, V>
where
    F: SegmentFile + Sync + Send + 'static,
    U: SegmentFileFactory<F> + Sync + Send + 'static,
    V: SegmentReaderFactory<F> + Sync + Send + 'static,
{
    fn drop(&mut self) {
        if let Some(handle) = self.merging_thread_join_handle.take() {
            let _ = handle.join();
        }
    }
}

impl<F, U, V> SegmentedFilesDb<F, U, V>
where
    F: SegmentFile + Sync + Send + 'static,
    U: SegmentFileFactory<F> + Sync + Send + 'static,
    V: SegmentReaderFactory<F> + Sync + Send + 'static,
{
    pub fn new(
        dir_path: &str,
        merging_threshold: u64,
        segment_creation_policy: SegmentCreationPolicy,
        file_factory: U,
        reader_factory: V,
    ) -> DbResult<Self> {
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
            current_segment: current_segment,
            reader_factory: Arc::new(reader_factory),
            file_factory: Arc::new(file_factory),
            merging_thread_join_handle: None,
        })
    }
    fn maybe_create_fresh_segment(&mut self) -> DbResult<()> {
        if is_thread_running(&self.merging_thread_join_handle) {
            return Ok(());
        }
        if let SegmentCreationPolicy::Automatic = self.segment_creation_policy {
            self.create_fresh_segment()?
        }
        Ok(())
    }
    fn maybe_merge_past_segments_in_background(&mut self) {
        if is_thread_running(&self.merging_thread_join_handle) {
            return;
        }

        let locked_past_segments = Arc::clone(&self.locked_past_segments);
        let file_factory = Arc::clone(&self.file_factory);
        let reader_factory = Arc::clone(&self.reader_factory);
        self.merging_thread_join_handle = Some(spawn(move || {
            if let Err(e) =
                Self::merge_past_segments(locked_past_segments, file_factory, reader_factory)
            {
                panic!("error in merging thread: {e}")
            }
        }));
    }
    fn merge_past_segments(
        locked_past_segments: Arc<RwLock<Vec<Segment<F>>>>,
        file_factory: Arc<U>,
        reader_factory: Arc<V>,
    ) -> DbResult<()> {
        let mut merged_segment_file = file_factory.new(TMP_SEGMENT_FILE_NAME)?;

        for segment in locked_past_segments.read()?.iter().rev() {
            let file = segment.locked_file.read()?;
            let mut segment_reader = reader_factory.new(&file)?;
            merged_segment_file.absorb(&mut segment_reader)?;
        }

        merged_segment_file.compact()?;

        {
            let mut past_segments = locked_past_segments.write()?;
            while !past_segments.is_empty() {
                let past_segment = past_segments.pop().unwrap();
                past_segment.locked_file.into_inner()?.delete()?;
            }

            past_segments.push(Segment::from_file(merged_segment_file, 0)?)
        }

        Ok(())
    }
}
