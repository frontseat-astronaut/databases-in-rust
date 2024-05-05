use std::{fs::read_dir, path::PathBuf, thread::JoinHandle};

use crate::error::Error;

#[macro_export]
macro_rules! check_kvdb_entry {
    ($kv_entry_entry: expr) => {
        match $kv_entry_entry {
            Some(Present(value)) => return Ok(Some(value.to_owned())),
            Some(Deleted) => return Ok(None),
            None => {}
        }
    };
}

pub fn process_dir_contents(
    dir_path: &str,
    process_dir_entry: &mut dyn FnMut(PathBuf) -> Result<(), Error>,
) -> Result<(), Error> {
    let contents = read_dir(dir_path)?;
    for dir_entry_result in contents {
        let dir_entry = dir_entry_result?;
        process_dir_entry(dir_entry.path())?;
    }
    Ok(())
}

pub fn is_thread_running<T>(maybe_handle: &Option<JoinHandle<T>>) -> bool {
    match maybe_handle {
        Some(handle) => !handle.is_finished(),
        None => false,
    }
}
