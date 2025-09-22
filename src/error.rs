use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::net::AddrParseError;
use std::num::ParseIntError;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;
use std::sync::mpsc;

#[cfg(feature = "native")]
use config::ConfigError;
use log::ParseLevelError;
use log::SetLoggerError;
#[cfg(feature = "native")]
use rustyline::error::ReadlineError;
use serde::Deserialize;
use serde::Serialize;
#[cfg(feature = "native")]
use tokio::sync::broadcast;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ErrMsg {
    msg: String,
    file: String,
    line: u32,
}

impl ErrMsg {
    pub fn new(msg: String, file: impl Into<String>, line: u32) -> Self {
        Self { msg, file: file.into(), line }
    }
}

impl Display for ErrMsg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            write!(f, "{} (at {}:{})", self.msg, self.file, self.line)
        } else {
            write!(f, "{}", self.msg)
        }
    }
}

// All except Internal are considered user-facing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {
    Internal(ErrMsg),
    Value(ErrMsg),
    ReadOnly,
    Serialization,
    Parse(ErrMsg),
    AlreadyExists(ErrMsg),
    Unimplemented(ErrMsg),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Internal(s)
            | Error::Value(s)
            | Error::Parse(s)
            | Error::AlreadyExists(s)
            | Error::Unimplemented(s) => {
                write!(f, "{}", s)
            }
            err => {
                write!(f, "{:?}", err)
            }
        }
    }
}

#[macro_export]
macro_rules! error {
    ($variant:ident, $($arg:tt)*) => {
        $crate::error::Error::$variant($crate::error::ErrMsg::new(format!($($arg)*), file!(), line!()))
    };
}

#[macro_export]
macro_rules! internal_err {
    ($($arg:tt)*) => {
        $crate::error!(Internal, $($arg)*)
    };
}

#[macro_export]
macro_rules! value_err {
    ($($arg:tt)*) => {
        $crate::error!(Value, $($arg)*)
    };
}

#[macro_export]
macro_rules! abort_err {
    ($($arg:tt)*) => {
        $crate::error!(Abort, $($arg)*)
    };
}

#[macro_export]
macro_rules! readonly_err {
    ($($arg:tt)*) => {
        $crate::error!(ReadOnly, $($arg)*)
    };
}

#[macro_export]
macro_rules! serialization_err {
    ($($arg:tt)*) => {
        $crate::error!(Serialization, $($arg)*)
    };
}

#[macro_export]
macro_rules! parse_err {
    ($($arg:tt)*) => {
        $crate::error!(Parse, $($arg)*)
    };
}

#[macro_export]
macro_rules! already_exists_err {
    ($($arg:tt)*) => {
        $crate::error!(AlreadyExists, $($arg)*)
    };
}

#[macro_export]
macro_rules! unimplemented_err {
    ($($arg:tt)*) => {
        $crate::error!(Unimplemented, $($arg)*)
    };
}

impl std::error::Error for Error {}

impl serde::de::Error for Error {
    fn custom<T>(err: T) -> Self
    where
        T: Display,
    {
        internal_err!("{}", err)
    }
}

impl serde::ser::Error for Error {
    fn custom<T>(err: T) -> Self
    where
        T: Display,
    {
        internal_err!("{}", err)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        internal_err!("{}", err)
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(err: std::array::TryFromSliceError) -> Self {
        internal_err!("{}", err)
    }
}

impl From<Box<bincode::ErrorKind>> for Error {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        internal_err!("{}", err)
    }
}

impl<T> From<mpsc::SendError<T>> for Error {
    fn from(err: mpsc::SendError<T>) -> Self {
        internal_err!("{}", err)
    }
}

impl From<mpsc::RecvError> for Error {
    fn from(err: mpsc::RecvError) -> Self {
        internal_err!("{}", err)
    }
}

#[cfg(feature = "native")]
impl<T> From<broadcast::error::SendError<T>> for Error {
    fn from(err: broadcast::error::SendError<T>) -> Self {
        internal_err!("{}", err)
    }
}

#[cfg(feature = "native")]
impl From<ConfigError> for Error {
    fn from(err: ConfigError) -> Self {
        internal_err!("{}", err)
    }
}

#[cfg(feature = "native")]
impl From<tokio::task::JoinError> for Error {
    fn from(err: tokio::task::JoinError) -> Self {
        internal_err!("{}", err)
    }
}

#[cfg(feature = "native")]
impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
        internal_err!("{}", err)
    }
}

#[cfg(feature = "native")]
impl<T> From<tokio::sync::mpsc::error::TrySendError<T>> for Error {
    fn from(err: tokio::sync::mpsc::error::TrySendError<T>) -> Self {
        internal_err!("{}", err)
    }
}

#[cfg(feature = "native")]
impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        internal_err!("{}", err)
    }
}

impl From<TryFromIntError> for Error {
    fn from(err: TryFromIntError) -> Self {
        internal_err!("{}", err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        internal_err!("{}", err)
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        internal_err!("{}", err)
    }
}

impl From<Vec<u8>> for Error {
    fn from(err: Vec<u8>) -> Self {
        internal_err!("{:?}", err)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Self {
        internal_err!("{}", err)
    }
}

impl From<regex::Error> for Error {
    fn from(err: regex::Error) -> Self {
        internal_err!("{}", err)
    }
}

impl From<ParseIntError> for Error {
    fn from(err: ParseIntError) -> Self {
        parse_err!("{}", err)
    }
}

impl From<std::fmt::Error> for Error {
    fn from(err: std::fmt::Error) -> Self {
        internal_err!("{}", err)
    }
}

impl From<AddrParseError> for Error {
    fn from(err: AddrParseError) -> Self {
        internal_err!("{}", err)
    }
}

impl From<ParseLevelError> for Error {
    fn from(err: ParseLevelError) -> Self {
        internal_err!("{}", err)
    }
}
impl From<SetLoggerError> for Error {
    fn from(err: SetLoggerError) -> Self {
        internal_err!("{}", err)
    }
}

#[cfg(feature = "native")]
impl From<ReadlineError> for Error {
    fn from(err: ReadlineError) -> Self {
        internal_err!("{}", err)
    }
}

impl From<Error> for std::fmt::Error {
    fn from(_err: Error) -> Self {
        std::fmt::Error
    }
}
