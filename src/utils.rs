use std::{fs::read_dir, path::PathBuf};

use crate::error::Error;

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
    let contents = read_dir(dir_path)?;
    for dir_entry_result in contents {
        let dir_entry = dir_entry_result?;
        process_dir_entry(dir_entry.path())?;
    }
    Ok(())
}
