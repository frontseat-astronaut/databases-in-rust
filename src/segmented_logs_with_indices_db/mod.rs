use segment_file::ReaderFactory;

use self::segment_file::{Factory, File};
use crate::error::DbResult;
use crate::{
    kvdb::KVDb,
    segmented_files_db::{SegmentCreationPolicy, SegmentedFilesDb},
};

mod segment_file;

pub struct SegmentedLogsWithIndicesDb {
    description: String,
    segmented_files_db: SegmentedFilesDb<File, Factory, ReaderFactory>,
}

impl KVDb for SegmentedLogsWithIndicesDb {
    fn description(&self) -> String {
        self.description.clone()
    }
    fn set(&mut self, key: &str, value: &str) -> DbResult<()> {
        self.segmented_files_db.set(key, value)
    }
    fn delete(&mut self, key: &str) -> DbResult<()> {
        self.segmented_files_db.delete(key)
    }
    fn get(&mut self, key: &str) -> DbResult<Option<String>> {
        self.segmented_files_db.get(key)
    }
}

impl SegmentedLogsWithIndicesDb {
    pub fn new(
        dir_path: &str,
        file_size_threshold: u64,
        merging_threshold: u64,
    ) -> DbResult<SegmentedLogsWithIndicesDb> {
        let description = format!("Segmented logs with indices DB, with file size threshold of {} bytes and merging threshold of {} files", file_size_threshold, merging_threshold);
        Ok(SegmentedLogsWithIndicesDb {
            description,
            segmented_files_db: SegmentedFilesDb::<File, Factory, ReaderFactory>::new(
                dir_path,
                merging_threshold,
                SegmentCreationPolicy::Automatic,
                Factory {
                    dir_path: dir_path.to_owned(),
                    file_size_threshold,
                },
                ReaderFactory {},
            )?,
        })
    }
}
