use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, Write};
use crate::kvdb::{KVDb, error::Error};

pub struct LogDb;

const DELIMITER: &str = ",";
const DIR_PATH: &str = "db_files/log_db/";
const FILE_NAME: &str = "log.txt";

impl KVDb for LogDb {
    fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        open_file()
            .and_then(
                |mut file| {
                    write_line(&mut file, key, value)
                }
            )
    }
    fn get(&self, key: &str) -> Result<Option<String>, Error> {
        open_file()
            .and_then(
                |mut file| {
                    let mut value = None;
                    read_lines(&mut file, &mut |parsed_key, parsed_value, _| {
                        if parsed_key == key {
                            value = Some(parsed_value);
                        }
                        Ok(())
                    }).and(Ok(value))
                }
            )
    }
}

impl LogDb {
    pub fn new() -> LogDb {
        LogDb {}
    }
}

pub fn write_line(file: &mut File, key: &str, value: &str) -> Result<(), Error> {
    if let Err(e) = writeln!(file, "{}{}{}", key, DELIMITER, value) {
        return Err(Error::from_io_error(&e))
    }
    Ok(())
}

pub fn read_line(reader: &mut BufReader<&mut File>) -> Result<Option<(String, String)>, Error> {
    let mut buf = String::new();
    match reader.read_line(&mut buf) {
        Ok(0) => Ok(None),
        Ok(_) => {
            // remove \n
            let _ = buf.split_off(buf.len() - 1);
            match buf.split_once(DELIMITER) {
                Some((key, value)) => Ok(Some((key.to_string(), value.to_string()))),
                None => Err(
                        Error::new(
                            &format!("ill-formed line in file, expected the delimiter '{}' to be present", DELIMITER)
                        )
                    )
            }
        }
        Err(e) => Err(Error::from_io_error(&e))
    }
}

pub fn read_lines(file: &mut File, process_line: &mut dyn FnMut(String, String, u64) -> Result<(), Error>) -> Result<(), Error> {
    let mut reader = BufReader::new(file);
    loop {
        let result = match reader.stream_position() {
            Ok(offset) => {
                match read_line(&mut reader) {
                    Ok(None) => break,
                    Ok(Some((key, value))) => {
                        process_line(key, value, offset)
                    }
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(Error::from_io_error(&e)),
        };
        if result.is_err() {
            return result;
        }
    }
    Ok(())
}

fn open_file() -> Result<File, Error> {
    if let Err(e) = fs::create_dir_all(DIR_PATH) {
        return Err(Error::from_io_error(&e))
    }
    let file_path = DIR_PATH.to_owned() + FILE_NAME;
    OpenOptions::new()
        .read(true)
        .write(true)
        .append(true)
        .create(true)
        .open(file_path).map_err(|e| {
            Error::from_io_error(&e)
        })
}