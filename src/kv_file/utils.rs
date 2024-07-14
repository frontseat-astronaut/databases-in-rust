use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Write},
};

use crate::{error::Error, kvdb::KeyStatus};
use crate::error::DbResult;
use super::{DELIMITER, TOMBSTONE};

pub fn read_line<T: Read>(
    reader: &mut BufReader<T>,
) -> DbResult<Option<(String, KeyStatus<String>)>> {
    let mut buf = String::new();
    let bytes_read = reader.read_line(&mut buf)?;
    if bytes_read == 0 {
        return Ok(None);
    }
    // remove \n
    let _ = buf.split_off(buf.len() - 1);
    match buf.split_once(DELIMITER) {
        Some((key, read_value)) => {
            let mut status = KeyStatus::Deleted;
            if read_value != TOMBSTONE {
                status = KeyStatus::Present(read_value.to_string())
            }
            Ok(Some((key.to_string(), status)))
        }
        None => Err(Error::InvalidData(format!(
            "ill-formed line in file, expected the delimiter '{}' to be present",
            DELIMITER
        ))),
    }
}

pub fn write_line(file: &mut File, key: &str, value: &KeyStatus<String>) -> DbResult<()> {
    if key.contains(DELIMITER) {
        return Err(Error::InvalidInput(format!(
            "key must not have '{}'",
            DELIMITER
        )));
    }
    let written_value = match value {
        KeyStatus::Present(ref value) => {
            if value == TOMBSTONE {
                return Err(Error::InvalidInput(format!(
                    "storing {} as a value is not supported",
                    TOMBSTONE
                )));
            }
            value
        }
        KeyStatus::Deleted => TOMBSTONE,
    };
    writeln!(file, "{}{}{}", key, DELIMITER, written_value)?;
    Ok(())
}
