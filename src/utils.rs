use std::{
    fs::read_dir,
    path::PathBuf,
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crate::kvdb::error::Error;

#[macro_export]
macro_rules! unwrap_or_return_io_error {
    ($result_expr: expr) => {
        match $result_expr {
            Ok(value) => value,
            Err(e) => return Err(Error::from_io_error(e)),
        }
    };
}

#[macro_export]
macro_rules! check_kvdb_result {
    ($kv_entry_result: expr) => {
        match $kv_entry_result? {
            Some(Present(value)) => return Ok(Some(value)),
            Some(Deleted) => return Ok(None),
            None => {}
        }
    };
}

pub fn process_dir_contents(
    dir_path: &str,
    process_dir_entry: &mut dyn FnMut(PathBuf) -> Result<(), Error>,
) -> Result<(), Error> {
    match read_dir(dir_path) {
        Ok(contents) => {
            for dir_entry_result in contents {
                match dir_entry_result {
                    Ok(dir_entry) => {
                        if let Err(e) = process_dir_entry(dir_entry.path()) {
                            return Err(e);
                        }
                    }
                    Err(e) => return Err(Error::from_io_error(e)),
                }
            }
            Ok(())
        }
        Err(e) => Err(Error::from_io_error(e)),
    }
}

pub fn read_locked<'a, T>(resource_lock: &'a RwLock<T>) -> Result<RwLockReadGuard<'a, T>, Error> {
    match resource_lock.read() {
        Ok(resource) => Ok(resource),
        Err(_) => Err(Error::new("internal error: lock poisoned")),
    }
}

pub fn write_locked<'a, T>(resource_lock: &'a RwLock<T>) -> Result<RwLockWriteGuard<'a, T>, Error> {
    match resource_lock.write() {
        Ok(resource) => Ok(resource),
        Err(_) => Err(Error::new("internal error: lock poisoned")),
    }
}
