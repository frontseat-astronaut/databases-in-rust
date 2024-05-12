use std::error;
use std::fmt;
use std::io;
use std::sync::PoisonError;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    LockPoisoned,
    InvalidInput(String),
    InvalidData(String),
    Wrapped(String, Box<Self>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref err) => write!(f, "I/O error: {}", err),
            Error::LockPoisoned => write!(f, "lock for resource poisoned"),
            Error::InvalidInput(ref msg) => write!(f, "invalid input error: {}", msg),
            Error::InvalidData(ref msg) => write!(f, "invalid data error: {}", msg),
            Error::Wrapped(ref msg, ref err) => write!(f, "{}: {}", msg, err),
        }
    }
}

impl error::Error for Error {
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            Error::Io(ref err) => Some(err),
            Error::LockPoisoned => None,
            Error::InvalidInput(_) => None,
            Error::InvalidData(_) => None,
            Error::Wrapped(_, ref err) => Some(err),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Error::LockPoisoned
    }
}

impl Error {
    pub fn wrap(msg: &str, err: Self) -> Self {
        Error::Wrapped(msg.to_string(), Box::new(err))
    }
}
