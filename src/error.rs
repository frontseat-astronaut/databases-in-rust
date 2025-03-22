use std::error;
use std::fmt;
use std::io;
use std::sync::PoisonError;

pub type DbResult<T> = Result<T, Error>;

#[derive(Debug)]
pub enum SerDeError {
    AvroError(apache_avro::Error),
    MessagePackDecodeError(rmp_serde::decode::Error),
    MessagePackEncodeError(rmp_serde::encode::Error),
    JSONError(serde_json::Error),
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    LockPoisoned,
    InvalidInput(String),
    SerDeError(SerDeError),
    Wrapped(String, Box<Self>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref err) => write!(f, "I/O error: {}", err),
            Error::LockPoisoned => write!(f, "lock for resource poisoned"),
            Error::InvalidInput(ref msg) => write!(f, "invalid input error: {}", msg),
            Error::SerDeError(ref err) => {
                write!(f, "error in serializing/deserializing: {:?}", err)
            }
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
            Error::SerDeError(ref err) => match err {
                SerDeError::AvroError(error) => Some(error),
                SerDeError::MessagePackEncodeError(error) => Some(error),
                SerDeError::MessagePackDecodeError(error) => Some(error),
                SerDeError::JSONError(error) => Some(error),
            },
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

impl From<apache_avro::Error> for Error {
    fn from(e: apache_avro::Error) -> Self {
        Error::SerDeError(SerDeError::AvroError(e))
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(e: rmp_serde::decode::Error) -> Self {
        Error::SerDeError(SerDeError::MessagePackDecodeError(e))
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Error::SerDeError(SerDeError::MessagePackEncodeError(e))
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::SerDeError(SerDeError::JSONError(e))
    }
}

impl Error {
    pub fn wrap(msg: &str, err: Self) -> Self {
        Error::Wrapped(msg.to_string(), Box::new(err))
    }
}
