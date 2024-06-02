use std::{fs::read_dir, path::PathBuf, thread::JoinHandle};

use crate::error::Error;

#[macro_export]
macro_rules! check_key_status {
    ($maybe_status: expr) => {
        match $maybe_status {
            Some(Present(value)) => return Ok(Some(value.to_owned())),
            Some(Deleted) => return Ok(None),
            None => {}
        }
    };
}

pub fn process_dir_contents(
    dir_path: &str,
    process_dir_status: &mut dyn FnMut(PathBuf) -> Result<(), Error>,
) -> Result<(), Error> {
    let contents = read_dir(dir_path)?;
    for dir_status_result in contents {
        let dir_status = dir_status_result?;
        process_dir_status(dir_status.path())?;
    }
    Ok(())
}

pub fn is_thread_running<T>(maybe_handle: &Option<JoinHandle<T>>) -> bool {
    match maybe_handle {
        Some(handle) => !handle.is_finished(),
        None => false,
    }
}
