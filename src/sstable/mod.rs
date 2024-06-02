use std::{
    collections::BTreeMap,
    mem::swap,
    sync::{Arc, RwLock},
    thread::{spawn, JoinHandle},
};

use crate::{
    check_kvdb_entry,
    error::Error,
    kv_file::KVFile,
    kvdb::{
        KVDb,
        KVEntry::{self, Deleted, Present},
    },
    segmented_files_db::{SegmentCreationPolicy, SegmentedFilesDb},
    utils::is_thread_running,
};

use self::segment_file::{Factory, File};

const MEMTABLE_BACKUP_FILE_NAME: &str = "memtable_backup.txt";
const TMP_MEMTABLE_BACKUP_FILE_NAME: &str = "tmp_memtable_backup.txt";

mod segment_file;

type Memtable = BTreeMap<String, KVEntry<String>>;

pub struct SSTable {
    memtable_size_threshold: usize,
    memtable: Memtable,
    memtable_backup: KVFile,
    locked_tmp_memtable: Arc<RwLock<Memtable>>,
    locked_tmp_memtable_backup: Arc<RwLock<KVFile>>,
    locked_segmented_files_db: Arc<RwLock<SegmentedFilesDb<File, Factory>>>,
    join_handle: Option<JoinHandle<()>>,
}

impl KVDb for SSTable {
    fn name(&self) -> String {
        "SS Table".to_string()
    }
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.flush_memtable_if_big().and_then(|_| {
            let entry = Present(value.to_string());
            if let Err(e) = self.memtable_backup.append_line(key, &entry) {
                println!("error in writing to memtable backup: {}", e);
            }
            self.memtable.insert(key.to_string(), entry);
            Ok(())
        })
    }
    fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.flush_memtable_if_big().and_then(|_| {
            if let Err(e) = self.memtable_backup.append_line(key, &Deleted) {
                println!("error in writing to memtable backup: {}", e);
            }
            self.memtable.insert(key.to_string(), Deleted);
            Ok(())
        })
    }
    fn get(&self, key: &str) -> Result<Option<String>, Error> {
        check_kvdb_entry!(self.memtable.get(key));
        {
            let tmp_memtable = self.locked_tmp_memtable.read()?;
            check_kvdb_entry!(tmp_memtable.get(key));
        }
        self.locked_segmented_files_db.read()?.get(key)
    }
}

impl Drop for SSTable {
    fn drop(&mut self) {
        if let Some(handle) = self.join_handle.take() {
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
    ) -> Result<Self, Error> {
        let (memtable, memtable_backup) =
            Self::recover_memtable_from_backup(dir_path, MEMTABLE_BACKUP_FILE_NAME)?;
        let (tmp_memtable, tmp_memtable_backup) =
            Self::recover_memtable_from_backup(dir_path, TMP_MEMTABLE_BACKUP_FILE_NAME)?;
        Ok(SSTable {
            memtable_size_threshold,
            memtable,
            memtable_backup,
            locked_tmp_memtable: Arc::new(RwLock::new(tmp_memtable)),
            locked_tmp_memtable_backup: Arc::new(RwLock::new(tmp_memtable_backup)),
            locked_segmented_files_db: Arc::new(RwLock::new(
                SegmentedFilesDb::<File, Factory>::new(
                    dir_path,
                    merging_threshold,
                    SegmentCreationPolicy::Triggered,
                    Factory {
                        dir_path: dir_path.to_owned(),
                        sparsity,
                    },
                )?,
            )),
            join_handle: None,
        })
    }
    fn flush_memtable_if_big(&mut self) -> Result<(), Error> {
        if self.memtable.len() >= self.memtable_size_threshold {
            let moved = self.try_moving_data_to_tmp_memtable()?;
            if moved {
                self.flush_tmp_memtable_in_background();
            }
        }
        Ok(())
    }
    fn try_moving_data_to_tmp_memtable(&mut self) -> Result<bool, Error> {
        if is_thread_running(&self.join_handle) {
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
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
        let locked_tmp_memtable = Arc::clone(&self.locked_tmp_memtable);
        let locked_tmp_memtable_backup = Arc::clone(&self.locked_tmp_memtable_backup);
        let locked_segmented_files_db = Arc::clone(&self.locked_segmented_files_db);
        self.join_handle = Some(spawn(move || {
            if let Err(e) = Self::flush_tmp_memtable(
                locked_tmp_memtable,
                locked_tmp_memtable_backup,
                locked_segmented_files_db,
            ) {
                println!("error in flushing memtable: {}", e);
            }
        }));
    }
    fn flush_tmp_memtable(
        locked_tmp_memtable: Arc<RwLock<Memtable>>,
        locked_tmp_memtable_backup: Arc<RwLock<KVFile>>,
        locked_segmented_files_db: Arc<RwLock<SegmentedFilesDb<File, Factory>>>,
    ) -> Result<(), Error> {
        {
            let tmp_memtable = locked_tmp_memtable.read()?;
            if tmp_memtable.is_empty() {
                return Ok(());
            }

            let mut segmented_files_db = locked_segmented_files_db.write()?;
            segmented_files_db
                .create_fresh_segment()
                .map_err(|e| Error::wrap("error in creating fresh segment", e))?;

            for (key, entry) in tmp_memtable.iter() {
                match entry {
                    Present(value) => segmented_files_db.set(key, value)?,
                    Deleted => segmented_files_db.delete(key)?,
                }
            }
        }
        locked_tmp_memtable.write()?.clear();
        locked_tmp_memtable_backup.write()?.delete()?;

        Ok(())
    }
    fn swap_memtable_backup_files(&mut self) -> Result<(), Error> {
        {
            let mut tmp_memtable_backup = self.locked_tmp_memtable_backup.write()?;
            swap(&mut (*tmp_memtable_backup), &mut self.memtable_backup);
            self.memtable_backup.rename("TMP_FILE.txt")?;
            tmp_memtable_backup.rename(TMP_MEMTABLE_BACKUP_FILE_NAME)?;
        }
        self.memtable_backup.rename(MEMTABLE_BACKUP_FILE_NAME)?;
        Ok(())
    }
    fn recover_memtable_from_backup(
        dir_path: &str,
        file_name: &str,
    ) -> Result<(Memtable, KVFile), Error> {
        let backup = KVFile::new(dir_path, file_name);
        let mut memtable = Memtable::new();
        for line_result in backup.iter()? {
            let line = line_result?;
            memtable.insert(line.key, line.entry);
        }
        Ok((memtable, backup))
    }
}
