use std::fmt::{Debug, Display, Formatter};
use std::num::TryFromIntError;
use std::string::FromUtf8Error;
use std::sync::mpsc;

use config::ConfigError;
use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, Error>;

// All except Internal are considered user-facing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {
    Internal(String),
    Value(String),
    BufferPoolNoAvailableFrame,
    Abort,
}

impl Error {
    pub fn internal<E: ToString>(msg: E) -> Error {
        Error::Internal(msg.to_string())
    }

    pub fn value<E: ToString>(msg: E) -> Error {
        Error::Value(msg.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Abort => write!(f, "Operation aborted"),
            Error::Internal(s) | Error::Value(s) => {
                write!(f, "{}", s)
            }
            err => {
                write!(f, "{:?}", err)
            }
        }
    }
}

impl std::error::Error for Error {}

impl serde::de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Error::internal(msg)
    }
}

impl serde::ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Error::internal(msg)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        Error::internal(err)
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(err: std::array::TryFromSliceError) -> Self {
        Error::internal(err)
    }
}

impl From<Box<bincode::ErrorKind>> for Error {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        Error::internal(err)
    }
}

impl<T> From<mpsc::SendError<T>> for Error {
    fn from(err: mpsc::SendError<T>) -> Self {
        Error::internal(err)
    }
}

impl From<mpsc::RecvError> for Error {
    fn from(err: mpsc::RecvError) -> Self {
        Error::internal(err)
    }
}

impl From<TryFromIntError> for Error {
    fn from(err: TryFromIntError) -> Self {
        Error::internal(err)
    }
}

impl From<ConfigError> for Error {
    fn from(err: ConfigError) -> Self {
        Error::internal(err)
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(err: tokio::task::JoinError) -> Self {
        Error::internal(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::internal(err)
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Error::internal(err)
    }
}

impl<T> From<tokio::sync::mpsc::error::TrySendError<T>> for Error {
    fn from(err: tokio::sync::mpsc::error::TrySendError<T>) -> Self {
        Error::internal(err)
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        Error::internal(err)
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        Error::internal(err)
    }
}

impl From<Vec<u8>> for Error {
    fn from(err: Vec<u8>) -> Self {
        Error::internal(format!("{:?}", err))
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Self {
        Error::internal(err)
    }
}
