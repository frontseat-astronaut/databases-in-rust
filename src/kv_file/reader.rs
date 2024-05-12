use std::{fs::File, io::BufReader};

use crate::{error::Error, kvdb::KVEntry};

use super::utils::read_line;

#[derive(Debug)]
pub struct KVFileReader {
    pub reader: BufReader<File>,
}

impl KVFileReader {
    pub fn read_line(&mut self) -> Result<Option<(String, KVEntry<String>)>, Error> {
        read_line(&mut self.reader)
    }
}
