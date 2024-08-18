use std::{
    collections::BTreeMap,
    mem::swap,
    sync::{Arc, Mutex, RwLock},
    thread::{spawn, JoinHandle},
};

use self::segment_file::{Factory, File, ReaderFactory};
use crate::error::DbResult;
use crate::tmp_file_names::{
    MEMTABLE_BACKUP_FILE_NAME, TMP_MEMTABLE_BACKUP_FILE_NAME, TMP_MEMTABLE_BACKUP_SWAP_FILE_NAME,
};
use crate::{
    check_key_status,
    error::Error,
    kv_file::KVFile,
    kvdb::{
        KVDb,
        KeyStatus::{self, Deleted, Present},
    },
    segmented_files_db::{SegmentCreationPolicy, SegmentedFilesDb},
    utils::is_thread_running,
};

mod segment_file;

type Memtable = BTreeMap<String, KeyStatus<String>>;

pub struct SSTable {
    description: String,
    memtable_size_threshold: usize,
    memtable: Memtable,
    memtable_backup: KVFile,
    locked_tmp_memtable: Arc<RwLock<Memtable>>,
    locked_tmp_memtable_backup: Arc<RwLock<KVFile>>,
    locked_segmented_files_db: Arc<Mutex<SegmentedFilesDb<File, Factory, ReaderFactory>>>,
    flush_memtable_thread_join_handle: Option<JoinHandle<()>>,
}

impl KVDb for SSTable {
    fn description(&self) -> String {
        self.description.clone()
    }
    fn set(&mut self, key: &str, value: &str) -> DbResult<()> {
        self.flush_memtable_if_big().and_then(|_| {
            let status = Present(value.to_string());
            if let Err(e) = self.memtable_backup.append_line(key, &status) {
                println!("error in writing to memtable backup: {}", e);
            }
            self.memtable.insert(key.to_string(), status);
            Ok(())
        })
    }
    fn delete(&mut self, key: &str) -> DbResult<()> {
        self.flush_memtable_if_big().and_then(|_| {
            if let Err(e) = self.memtable_backup.append_line(key, &Deleted) {
                println!("error in writing to memtable backup: {}", e);
            }
            self.memtable.insert(key.to_string(), Deleted);
            Ok(())
        })
    }
    fn get(&mut self, key: &str) -> DbResult<Option<String>> {
        check_key_status!(self.memtable.get(key));
        {
            let tmp_memtable = self.locked_tmp_memtable.read()?;
            check_key_status!(tmp_memtable.get(key));
        }
        self.locked_segmented_files_db.lock()?.get(key)
    }
}

impl Drop for SSTable {
    fn drop(&mut self) {
        if let Some(handle) = self.flush_memtable_thread_join_handle.take() {
            let _ = handle.join();
        }
    }
}

impl SSTable {
    pub fn new(
        dir_path: &str,
        merging_threshold: u64,
        sparsity: u64,
        memtable_size_threshold: usize,
    ) -> DbResult<Self> {
        let description = format!("SS Table with merging threshold of {} files, sparsity of {} bytes and memtable size threshold of {} keys",
            merging_threshold, sparsity, memtable_size_threshold
        );
        let (memtable, memtable_backup) =
            Self::recover_memtable_from_backup(dir_path, MEMTABLE_BACKUP_FILE_NAME)?;
        let (tmp_memtable, tmp_memtable_backup) =
            Self::recover_memtable_from_backup(dir_path, TMP_MEMTABLE_BACKUP_FILE_NAME)?;
        Ok(SSTable {
            description,
            memtable_size_threshold,
            memtable,
            memtable_backup,
            locked_tmp_memtable: Arc::new(RwLock::new(tmp_memtable)),
            locked_tmp_memtable_backup: Arc::new(RwLock::new(tmp_memtable_backup)),
            locked_segmented_files_db: Arc::new(Mutex::new(SegmentedFilesDb::<
                File,
                Factory,
                ReaderFactory,
            >::new(
                dir_path,
                merging_threshold,
                SegmentCreationPolicy::Triggered,
                Factory {
                    dir_path: dir_path.to_owned(),
                    sparsity,
                },
                ReaderFactory {},
            )?)),
            flush_memtable_thread_join_handle: None,
        })
    }
    fn flush_memtable_if_big(&mut self) -> DbResult<()> {
        if self.memtable.len() >= self.memtable_size_threshold {
            let moved = self.try_moving_data_to_tmp_memtable()?;
            if moved {
                self.flush_tmp_memtable_in_background();
            }
        }
        Ok(())
    }
    fn try_moving_data_to_tmp_memtable(&mut self) -> DbResult<bool> {
        if is_thread_running(&self.flush_memtable_thread_join_handle) {
            return Ok(false);
        }
        let should_move;
        {
            let mut tmp_memtable = self.locked_tmp_memtable.write()?;
            should_move = tmp_memtable.is_empty();
            if should_move {
                swap(&mut (*tmp_memtable), &mut self.memtable);
            }
        }
        if should_move {
            if let Err(e) = self.swap_memtable_backup_files() {
                println!("error in swapping memtable backup files: {}", e);
            }
        }
        Ok(should_move)
    }
    fn flush_tmp_memtable_in_background(&mut self) {
        assert_eq!(
            is_thread_running(&self.flush_memtable_thread_join_handle),
            false
        );
        let locked_tmp_memtable = Arc::clone(&self.locked_tmp_memtable);
        let locked_tmp_memtable_backup = Arc::clone(&self.locked_tmp_memtable_backup);
        let locked_segmented_files_db = Arc::clone(&self.locked_segmented_files_db);
        self.flush_memtable_thread_join_handle = Some(spawn(move || {
            if let Err(e) = Self::flush_tmp_memtable(
                locked_tmp_memtable,
                locked_tmp_memtable_backup,
                locked_segmented_files_db,
            ) {
                panic!("error in flushing memtable: {}", e);
            }
        }));
    }
    fn flush_tmp_memtable(
        locked_tmp_memtable: Arc<RwLock<Memtable>>,
        locked_tmp_memtable_backup: Arc<RwLock<KVFile>>,
        locked_segmented_files_db: Arc<Mutex<SegmentedFilesDb<File, Factory, ReaderFactory>>>,
    ) -> DbResult<()> {
        {
            let tmp_memtable = locked_tmp_memtable.read()?;
            if tmp_memtable.is_empty() {
                return Ok(());
            }

            let mut segmented_files_db = locked_segmented_files_db.lock()?;
            segmented_files_db
                .create_fresh_segment()
                .map_err(|e| Error::wrap("error in creating fresh segment", e))?;

            for (key, status) in tmp_memtable.iter() {
                match status {
                    Present(value) => segmented_files_db.set(key, value)?,
                    Deleted => segmented_files_db.delete(key)?,
                }
            }
        }
        locked_tmp_memtable.write()?.clear();
        locked_tmp_memtable_backup.write()?.delete()?;

        Ok(())
    }
    fn swap_memtable_backup_files(&mut self) -> DbResult<()> {
        {
            let mut tmp_memtable_backup = self.locked_tmp_memtable_backup.write()?;
            swap(&mut (*tmp_memtable_backup), &mut self.memtable_backup);
            self.memtable_backup
                .rename(TMP_MEMTABLE_BACKUP_SWAP_FILE_NAME)?;
            tmp_memtable_backup.rename(TMP_MEMTABLE_BACKUP_FILE_NAME)?;
        }
        self.memtable_backup.rename(MEMTABLE_BACKUP_FILE_NAME)?;
        Ok(())
    }
    fn recover_memtable_from_backup(
        dir_path: &str,
        file_name: &str,
    ) -> DbResult<(Memtable, KVFile)> {
        let mut backup = KVFile::new(dir_path, file_name)?;
        let mut memtable = Memtable::new();
        for line_result in backup.iter()? {
            let line = line_result?;
            memtable.insert(line.key, line.status);
        }
        Ok((memtable, backup))
    }
}
