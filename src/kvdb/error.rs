use std::fmt;
use std::io;

#[derive(Debug)]
pub struct Error {
    msg: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl Error {
    pub fn new(msg: &str) -> Error {
        Error {
            msg: String::from(msg),
        }
    }
    pub fn from_io_error(e: &io::Error) -> Error {
        Error { msg: e.to_string() }
    }
}
