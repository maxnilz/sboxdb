#![allow(unused_variables)]

//! keyCodec is a lexicographical order-preserving binary encoding for use with
//! keys. It is designed for simplicity, not efficiency (i.e. it does not use
//! varints or other compression method).
//!
//! The encoding is not self-describing: the caller must provide a concrete type
//! to decode into, and the binary key must conform to its structure.
//!
//! KeyCodec supports a subset of primitive data types, encoded as follows:
//!
//! u64: Big-endian binary representation
//! i64: Big-endian binary representation where the sign bit get flipped
//! bytes(Vec<u8>): 0x00 is escaped as 0x00ff, terminated with 0x0000
//! String: Same as the bytes(Vec<u8>)
//!
//! lexicographical compare rules are:
//! 1. compare byte by byte from left to right
//! 2. when all bytes in an array match the beginning of a longer string, the
//!    shorter array is considered smaller. e.g., "app" smaller than "apple".

use serde::de::value::U32Deserializer;
use serde::de::{DeserializeSeed, EnumAccess, IntoDeserializer, VariantAccess, Visitor};
use serde::ser;

use crate::error::{Error, Result};

pub fn serialize<T: serde::Serialize>(key: &T) -> Result<Vec<u8>> {
    let mut serializer = Serializer { data: Vec::new() };
    key.serialize(&mut serializer)?;
    Ok(serializer.data)
}

pub fn deserialize<'a, T: serde::Deserialize<'a>>(input: &'a [u8]) -> Result<T> {
    let mut deserializer = Deserializer::from_bytes(input);
    let t = T::deserialize(&mut deserializer)?;
    Ok(t)
}

struct Serializer {
    data: Vec<u8>,
}

impl serde::Serializer for &mut Serializer {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = ser::Impossible<(), Error>;
    type SerializeTuple = ser::Impossible<(), Error>;
    type SerializeTupleStruct = ser::Impossible<(), Error>;
    type SerializeTupleVariant = ser::Impossible<(), Error>;
    type SerializeMap = ser::Impossible<(), Error>;
    type SerializeStruct = ser::Impossible<(), Error>;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    fn serialize_bool(self, v: bool) -> Result<()> {
        todo!()
    }

    fn serialize_i8(self, _: i8) -> Result<()> {
        todo!()
    }

    fn serialize_i16(self, _: i16) -> Result<()> {
        todo!()
    }

    fn serialize_i32(self, _: i32) -> Result<()> {
        todo!()
    }

    /// i64 uses the big-endian two's completement encoding, but flips the
    /// left-most sign bit such that negative numbers are ordered before
    /// positive numbers.
    ///
    /// The relative ordering of the remaining bits is already correct: -1, the
    /// largest negative integer, is encoded as 01111111...11111111, ordered
    /// after all other negative integers but before positive integers.
    fn serialize_i64(self, v: i64) -> Result<()> {
        let mut bytes = v.to_be_bytes();
        bytes[0] ^= 1 << 7; // flip sign bit
        self.data.extend(bytes);
        Ok(())
    }

    fn serialize_u8(self, _: u8) -> Result<()> {
        todo!()
    }

    fn serialize_u16(self, _: u16) -> Result<()> {
        todo!()
    }

    fn serialize_u32(self, _: u32) -> Result<()> {
        todo!()
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.data.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_f32(self, _: f32) -> Result<()> {
        todo!()
    }

    fn serialize_f64(self, _: f64) -> Result<()> {
        todo!()
    }

    fn serialize_char(self, _: char) -> Result<()> {
        todo!()
    }

    // Strings are encoded like bytes.
    fn serialize_str(self, v: &str) -> Result<()> {
        self.serialize_bytes(v.as_bytes())
    }

    // Byte slices are terminated by 0x0000, escaping 0x00 as 0x00ff.
    // Prefix-length encoding can't be used, since it violates ordering.
    //
    // By terminating with 0x0000 and escaping any 0x00 bytes as 0x00ff,
    // the system ensures that the end of data markers do not appear in
    // the data and that the data itself remains sortable in its natural
    // order without interference from a length prefix.
    //
    // This approach allows data to be reliably and correctly ordered,
    // retrieved, and processed based on its byte-wise contents.
    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        let bytes = v
            .iter()
            .flat_map(|b| match b {
                0x00 => vec![0x00, 0xff],
                b => vec![*b],
            })
            .chain(vec![0x00, 0x00]);
        self.data.extend(bytes);
        Ok(())
    }

    fn serialize_none(self) -> Result<()> {
        todo!()
    }

    fn serialize_some<T: ?Sized>(self, _: &T) -> Result<()>
    where
        T: serde::Serialize,
    {
        todo!()
    }

    fn serialize_unit(self) -> Result<()> {
        todo!()
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<()> {
        todo!()
    }

    fn serialize_unit_variant(self, _: &'static str, index: u32, _: &'static str) -> Result<()> {
        let byte = u8::try_from(index)?;
        self.data.push(byte);
        Ok(())
    }

    fn serialize_newtype_struct<T: ?Sized>(self, _: &'static str, _: &T) -> Result<()>
    where
        T: serde::Serialize,
    {
        todo!()
    }

    // Newtype variants are serialized using the variant index and inner type.
    fn serialize_newtype_variant<T: ?Sized>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: serde::Serialize,
    {
        self.serialize_unit_variant(name, variant_index, variant)?;
        value.serialize(self)?;
        Ok(())
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq> {
        todo!()
    }

    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple> {
        todo!()
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        todo!()
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        todo!()
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        todo!()
    }

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        todo!()
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant> {
        todo!()
    }
}

struct Deserializer<'de> {
    input: &'de [u8],
}

impl<'de> Deserializer<'de> {
    pub fn from_bytes(input: &'de [u8]) -> Self {
        Deserializer { input }
    }

    fn take_bytes(&mut self, length: usize) -> Result<&[u8]> {
        if self.input.len() < length {
            return Err(Error::internal(format!(
                "insufficient bytes, except at least {} for {:x?}",
                length, self.input
            )));
        }
        let bytes = &self.input[..length];
        self.input = &self.input[length..];
        Ok(bytes)
    }

    fn decode_next_bytes(&mut self) -> Result<Vec<u8>> {
        let mut ans = Vec::new();
        let mut iter = self.input.iter().enumerate();
        let taken = loop {
            match iter.next() {
                Some((_, 0x00)) => match iter.next() {
                    Some((i, 0x00)) => break i + 1,
                    Some((_, 0xff)) => ans.push(0x00),
                    _ => return Err(Error::value("invalid escape sequence")),
                },
                Some((_, b)) => ans.push(*b),
                _ => return Err(Error::value("unexpected end of input")),
            }
        };
        self.input = &self.input[taken..];
        Ok(ans)
    }
}

impl<'de> serde::Deserializer<'de> for &mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bool<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i8<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i16<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i32<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let mut bytes = self.take_bytes(8)?.to_vec();
        bytes[0] ^= 1 << 7; // flip sign bit
        let i64 = i64::from_be_bytes(bytes.as_slice().try_into()?);
        visitor.visit_i64(i64)
    }

    fn deserialize_u8<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u16<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u32<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let bytes = self.take_bytes(8)?;
        let u64 = u64::from_be_bytes(bytes.try_into()?);
        visitor.visit_u64(u64)
    }

    fn deserialize_f32<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f64<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_char<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let bytes = self.decode_next_bytes()?;
        visitor.visit_str(&String::from_utf8(bytes)?)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let bytes = self.decode_next_bytes()?;
        visitor.visit_string(String::from_utf8(bytes)?)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let bytes = self.decode_next_bytes()?;
        visitor.visit_bytes(&bytes)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let bytes = self.decode_next_bytes()?;
        visitor.visit_byte_buf(bytes)
    }

    fn deserialize_option<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(self, _: &'static str, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(self, _: &'static str, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_seq<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, _: usize, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(self, _: &'static str, _: usize, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_struct<V>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        _: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_enum<V>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, _: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }
}

impl<'de> EnumAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: DeserializeSeed<'de>,
    {
        let index = self.take_bytes(1)?[0] as u32;
        let u32_deserializer: U32Deserializer<Error> = index.into_deserializer();
        let value = seed.deserialize(u32_deserializer)?;
        Ok((value, self))
    }
}

impl<'de> VariantAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(self)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn struct_variant<V>(self, fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use paste::paste;
    use serde::{Deserialize, Serialize};
    use serde_bytes::ByteBuf;

    use super::*;

    type Index = u64;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum Key {
        Unit,
        NewType0(Index),
        NewType1(String),
    }

    fn hex(input: &[u8]) -> String {
        let ans = input.iter().map(|b| format!("{:02x}", b)).collect::<String>();
        format!("0x{ans}")
    }

    macro_rules! test_codec {
        ( $($name:ident: $input:expr => $expect:literal),* ) => {
            paste!{
            $(
                #[test]
                fn [< test_ $name >]() -> Result<()> {
                    let mut input = $input;
                    let expect = $expect;
                    let output = serialize(&input)?;
                    assert_eq!(expect, hex(&output), "encode failed");

                    let expect = input;
                    // reuse input so that the compiler can infer the type.
                    input = deserialize(&output)?;
                    assert_eq!(expect, input, "decode failed");
                    Ok(())
                }
            )*
            }
        };
        ( $($name:ident: $input:expr => $expect:literal),+ ,) => {
            test_codec!{ $($name: $input => $expect),* }
        };
    }

    test_codec! {
        i64_min: i64::MIN => "0x0000000000000000",
        i64_neg_65535: -65535i64 => "0x7fffffffffff0001",
        i64_neg_1: -1i64 => "0x7fffffffffffffff",
        i64_neg_2: -2i64 => "0x7ffffffffffffffe",
        i64_neg_20: -20i64 => "0x7fffffffffffffec",
        i64_neg_256: -256i64 => "0x7fffffffffffff00",
        i64_neg_65536: -65536i64 => "0x7fffffffffff0000",
        i64_neg_16777216: -16777216i64 => "0x7fffffffff000000",
        i64_0: 0i64 => "0x8000000000000000",
        i64_1: 1i64 => "0x8000000000000001",
        i64_2: 2i64 => "0x8000000000000002",
        i64_20: 20i64 => "0x8000000000000014",
        i64_256: 256i64 => "0x8000000000000100",
        i64_65535: 65535i64 => "0x800000000000ffff",
        i64_max: i64::MAX => "0xffffffffffffffff",

        // compare different type keys(i64, u64)
        // although it is not raise error, the
        // result may not as what you expected.
        // use it with caution.

        u64_0: 0u64 => "0x0000000000000000",
        u64_1: 1u64 => "0x0000000000000001",
        u64_2: 2u64 => "0x0000000000000002",
        u64_20: 20u64 => "0x0000000000000014",
        u64_256: 256u64 => "0x0000000000000100",
        u64_max: u64::MAX => "0xffffffffffffffff",

        index_0: 0u64 as Index => "0x0000000000000000",

        bytes: ByteBuf::from(vec![0x01u8, 0xffu8]) => "0x01ff0000",
        bytes_empty: ByteBuf::from(vec![]) => "0x0000",
        bytes_escape: ByteBuf::from(vec![0x00, 0x01, 0x02]) => "0x00ff01020000",

        string: "foo".to_string() => "0x666f6f0000",
        string_empty: "".to_string() => "0x0000",
        string_escape: "foo\x00bar".to_string() => "0x666f6f00ff6261720000",
        string_utf8: "👋".to_string() => "0xf09f918b0000",

        enum_unit: Key::Unit => "0x00",
        enum_newtype0: Key::NewType0(0u64 as Index) => "0x010000000000000000",
        enum_newtype1: Key::NewType1("foo".to_string()) => "0x02666f6f0000",
    }
}
