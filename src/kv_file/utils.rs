use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Write},
};

use crate::{error::Error, kvdb::KVEntry};

use super::{DELIMITER, TOMBSTONE};

pub fn read_line<T: Read>(
    reader: &mut BufReader<T>,
) -> Result<Option<(String, KVEntry<String>)>, Error> {
    let mut buf = String::new();
    let bytes_read = reader.read_line(&mut buf)?;
    if bytes_read == 0 {
        return Ok(None);
    }
    // remove \n
    let _ = buf.split_off(buf.len() - 1);
    match buf.split_once(DELIMITER) {
        Some((key, read_value)) => {
            let mut value = KVEntry::Deleted;
            if read_value != TOMBSTONE {
                value = KVEntry::Present(read_value.to_string())
            }
            Ok(Some((key.to_string(), value)))
        }
        None => Err(Error::InvalidData(format!(
            "ill-formed line in file, expected the delimiter '{}' to be present",
            DELIMITER
        ))),
    }
}

pub fn write_line(file: &mut File, key: &str, value: &KVEntry<String>) -> Result<(), Error> {
    if key.contains(DELIMITER) {
        return Err(Error::InvalidInput(format!(
            "key must not have '{}'",
            DELIMITER
        )));
    }
    let written_value = match value {
        KVEntry::Present(ref value) => {
            if value == TOMBSTONE {
                return Err(Error::InvalidInput(format!(
                    "storing {} as a value is not supported",
                    TOMBSTONE
                )));
            }
            value
        }
        KVEntry::Deleted => TOMBSTONE,
    };
    writeln!(file, "{}{}{}", key, DELIMITER, written_value)?;
    Ok(())
}
