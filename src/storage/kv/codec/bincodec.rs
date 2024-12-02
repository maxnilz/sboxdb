//! Bincodec is binary encoding for rust values. For details, see:
//! https://github.com/bincode-org/bincode
//!
//! By default, the bincode::(de)serialize functions use fixed-length integer
//! encoding, despite DefaultOptions using variable-length encoding. This module
//! provides simple wrappers for these functions that use variable-length
//! encoding and the other defaults.
//!
//! Although bincode v2 has been released, however, it is still in development and
//! does not have a targeted MSRV yet. We are using bincode v1 currently.
//!
//! A noticeable breaking change from the lib user perspective is: the bincode v2 is
//! introducing Encode/Decode drive attributes for replacing the serde
//! Serialize/Deserialize attributes.
use bincode;
use bincode::{DefaultOptions, Options};
use serde::{Deserialize, Serialize};

use crate::error::Result;

pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let res = DefaultOptions::new().with_big_endian().with_varint_encoding().serialize(value)?;
    Ok(res)
}

pub fn deserialize<'a, T: Deserialize<'a>>(input: &'a [u8]) -> Result<T> {
    let res = DefaultOptions::new().with_big_endian().with_varint_encoding().deserialize(input)?;
    Ok(res)
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use serde::{Deserialize, Serialize};

    use crate::raft::Index;

    use super::*;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Entry<T> {
        index: Index,
        term: u64,
        command: T,
    }

    impl<T: Debug + PartialEq> Entry<T> {
        fn new(index: Index, term: u64, command: T) -> Self {
            Entry { index, term, command }
        }
    }

    #[test]
    fn test_codec() -> Result<()> {
        let entry = Entry::new(1, 1, "hello");
        let output = serialize(&entry)?;
        let got = deserialize(&output)?;
        assert_eq!(entry, got);
        Ok(())
    }
}
