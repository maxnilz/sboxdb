#![allow(unused_variables)]

//! keyCodec is a lexicographical order-preserving binary encoding for use with
//! keys. It is designed for simplicity, not efficiency (i.e. it does not use
//! varints or other compression method).
//!
//! The encoding is not self-describing: the caller must provide a concrete type
//! to decode into, and the binary key must conform to its structure.
//!
//! #### Supported Data Types
//!
//! ##### Unsigned Integers
//!
//! `u8`, `u16`, `u32`, and `u64` are encoded into 1, 2, 4, and 8 bytes of output, respectively.
//! Order is preserved by encoding the bytes in big-endian (most-significant bytes first) format.
//!
//! ##### Signed Integers
//!
//! `i8`, `i16`, `i32`, and `i64` are encoded into 1, 2, 4, and 8 bytes of output, respectively.
//! Order is preserved by flipping the sign bit of the most significant byte, such that negative
//! numbers are ordered before positive numbers.
//!
//! #### Vec<u8>
//!
//! 0x00 is escaped as 0x00ff, terminated with 0x0000. Prefix-length encoding can't be used,
//! since it violates ordering.
//!
//! ##### Strings
//!
//! Same as Vec<u8>, Strings are encoded into their natural UTF8 representation and terminated
//! with 0x0000, 0x00 is escaped as 0x00ff
//!
//! ##### Options
//!
//! An optional wrapper type adds a 1 byte overhead to the wrapped data type. `None` values will
//! sort before `Some` values.
//!
//! #### Structs & Tuples
//!
//! Structs and tuples are encoded by serializing their constituent fields in order with no prefix,
//! suffix, or padding bytes.
//!
//! ##### Enums
//!
//! Enums are encoded with a variable-length unsigned-integer variant tag, plus the constituent
//! fields in the case of an enum-struct. This encoding allows more enum variants to be added
//! in a backwards-compatible manner, as long as variants are not removed and the variant order
//! does not change.
//!
//! #### Unsupported Data Types
//!
//! Character are unsupported at this time. Characters are not typically useful in keys.
//!
//! Floating Point Numbers are unsupported at this time. In general, it is
//! unwise to use IEEE 754 floating point values in keys, because rounding errors are pervasive.
//!
//! Sequences and maps are unsupported at this time. Sequences and maps could probably be
//! implemented with a single byte overhead per item, key, and value, but these types are not
//! typically useful in keys.
//!
//! Raw byte arrays are unsupported out of box. The serde `Serializer`/`Deserializer` mechanism
//! makes no distinction between byte arrays and sequences, and thus the overhead for encoding a
//! raw byte array would be 1 byte per input byte. The theoretical best-case overhead for serializing
//! a raw (null containing) byte array in order-preserving format is 1 bit per byte, or 9 bytes of
//! output for every 8 bytes of input.
//! In case of non-null containing byte vectors and slices such as Vec<u8> that is guaranteed to
//! have no null bytes contained or the raw bytes does not to deserialize back, it can be wrapped
//! with serde_bytes::ByteBuf or use the #[serde(with="serde_bytes")] attribute.
//! See https://github.com/serde-rs/bytes
//!
//! NB: lexicographical compare rules are:
//! 1. compare byte by byte from left to right
//! 2. when all bytes in an array match the beginning of a longer string, the
//!    shorter array is considered smaller. e.g., "app" smaller than "apple".
//!
//! #### Type Evolution
//!
//! In general, the exact type of a serialized value must be known in order to correctly deserialize
//! it. For structs and enums, the type is effectively frozen once any values of the type have been
//! serialized: changes to the struct or enum will cause deserialization of already encoded values
//! to fail or return incorrect values. The only exception is adding new variants to the end
//! of an existing enum. Enum variants may *not* change type, be removed, or be reordered. All
//! changes to structs, including adding, removing, reordering, or changing the type of a field are
//! forbidden.
//!
//! Consider using an enum as a top-level wrapper type. This will allow us to seamlessly add a new
//! variant when we need to change the key format in a backwards-compatible manner (the different
//! key types will sort separately).

use serde::de;
use serde::de::IntoDeserializer;
use serde::ser;

use crate::error::Error;
use crate::error::Result;

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

impl Serializer {
    fn write_bytes(&mut self, bytes: &[u8]) {
        self.data.extend(bytes)
    }

    // Byte slices are terminated by 0x0000, escaping 0x00 as 0x00ff.
    // Prefix-length encoding can't be used, since it violates ordering.
    fn write_byte_slice(&mut self, bytes: &[u8]) {
        for &c in bytes.iter() {
            if c == 0x00 {
                self.data.extend(vec![0x00, 0xff])
            } else {
                self.data.push(c)
            }
        }
        self.data.extend(vec![0x00, 0x00])
    }

    fn write_byte(&mut self, b: u8) {
        self.data.push(b);
    }
}

struct Compound<'a> {
    ser: &'a mut Serializer,
}
impl ser::SerializeTuple for Compound<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl ser::SerializeTupleStruct for Compound<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl ser::SerializeTupleVariant for Compound<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl ser::SerializeStruct for Compound<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl ser::SerializeStructVariant for Compound<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> serde::Serializer for &'a mut Serializer {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = ser::Impossible<(), Error>;
    type SerializeTuple = Compound<'a>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Compound<'a>;
    type SerializeMap = ser::Impossible<(), Error>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Compound<'a>;

    fn serialize_bool(self, v: bool) -> Result<()> {
        let byte = if v { 1 } else { 0 };
        self.write_byte(byte);
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        let mut byte: u8 = v.to_be_bytes()[0];
        byte ^= 1 << 7; // flip sing bit to make negative number smaller
        self.write_byte(byte);
        Ok(())
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        let mut bytes = v.to_be_bytes();
        bytes[0] ^= 1 << 7;
        self.write_bytes(&bytes);
        Ok(())
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        let mut bytes = v.to_be_bytes();
        bytes[0] ^= 1 << 7;
        self.write_bytes(&bytes);
        Ok(())
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
        self.write_bytes(&bytes);
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.write_byte(v);
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        let bytes = v.to_be_bytes();
        self.write_bytes(&bytes);
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        let bytes = v.to_be_bytes();
        self.write_bytes(&bytes);
        Ok(())
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        let bytes = v.to_be_bytes();
        self.write_bytes(&bytes);
        Ok(())
    }

    fn serialize_f32(self, _: f32) -> Result<()> {
        unimplemented!()
    }

    fn serialize_f64(self, _: f64) -> Result<()> {
        unimplemented!()
    }

    fn serialize_char(self, v: char) -> Result<()> {
        unimplemented!()
    }

    // Strings are encoded like bytes.
    fn serialize_str(self, v: &str) -> Result<()> {
        self.serialize_bytes(v.as_bytes())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        self.write_byte_slice(v);
        Ok(())
    }

    fn serialize_none(self) -> Result<()> {
        self.write_byte(0x00);
        Ok(())
    }

    fn serialize_some<T>(self, v: &T) -> Result<()>
    where
        T: ?Sized + serde::Serialize,
    {
        self.write_byte(0x01);
        v.serialize(&mut *self)?;
        Ok(())
    }

    fn serialize_unit(self) -> Result<()> {
        Ok(())
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<()> {
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _: &'static str,
        variant_index: u32,
        _: &'static str,
    ) -> Result<()> {
        let byte = u8::try_from(variant_index)?;
        self.write_byte(byte);
        Ok(())
    }

    fn serialize_newtype_struct<T>(self, _: &'static str, v: &T) -> Result<()>
    where
        T: ?Sized + serde::Serialize,
    {
        v.serialize(self)?;
        Ok(())
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: ?Sized + serde::Serialize,
    {
        let byte = u8::try_from(variant_index)?;
        self.write_byte(byte);
        value.serialize(self)?;
        Ok(())
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq> {
        unimplemented!()
    }

    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple> {
        Ok(Compound { ser: self })
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Ok(Compound { ser: self })
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        let byte = u8::try_from(variant_index)?;
        self.write_byte(byte);
        Ok(Compound { ser: self })
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        unimplemented!()
    }

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        Ok(Compound { ser: self })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        let byte = u8::try_from(variant_index)?;
        self.write_byte(byte);
        Ok(Compound { ser: self })
    }
}

struct Deserializer<'de> {
    input: &'de [u8],
}

impl<'de> Deserializer<'de> {
    pub fn from_bytes(input: &'de [u8]) -> Self {
        Deserializer { input }
    }

    fn take_byte(&mut self) -> Result<u8> {
        let bytes = self.input;
        if bytes.is_empty() {
            return Err(Error::value("unexpected end of input"));
        }
        let byte = bytes[0];
        self.input = &bytes[1..];
        Ok(byte)
    }

    fn take_bytes(&mut self, n: usize) -> Result<&[u8]> {
        let bytes = self.input;
        if bytes.len() < n {
            return Err(Error::value("unexpected end of input"));
        }
        let (head, tail) = bytes.split_at(n);
        self.input = tail;
        Ok(head)
    }

    fn decode_byte_slice(&mut self) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        let mut iter = self.input.iter().enumerate();
        let taken = loop {
            match iter.next() {
                Some((_, 0x00)) => match iter.next() {
                    Some((i, 0x00)) => break i + 1,       // terminator
                    Some((_, 0xff)) => output.push(0x00), // escaped 0x00
                    _ => return Err(Error::value("invalid escape sequence")),
                },
                Some((_, &c)) => output.push(c),
                None => return Err(Error::value("unexpected end of input")),
            }
        };
        self.input = &self.input[taken..];
        Ok(output)
    }
}

impl<'de> serde::Deserializer<'de> for &mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, _: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let byte = self.take_byte()?;
        let bool = match byte {
            0x00 => false,
            0x01 => true,
            b => return Err(Error::internal(format!("invalid boolean value {:?}", b))),
        };
        visitor.visit_bool(bool)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let mut byte = self.take_byte()?;
        byte ^= 1 << 7; // flip sign bit
        visitor.visit_i8(byte as i8)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let mut bytes = self.take_bytes(2)?.to_vec();
        bytes[0] ^= 1 << 7;
        let i16 = i16::from_be_bytes(bytes.try_into()?);
        visitor.visit_i16(i16)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let mut bytes = self.take_bytes(4)?.to_vec();
        bytes[0] ^= 1 << 7; // flip sign bit
        let i32 = i32::from_be_bytes(bytes.try_into()?);
        visitor.visit_i32(i32)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let mut bytes = self.take_bytes(8)?.to_vec();
        bytes[0] ^= 1 << 7; // flip sign bit
        let i64 = i64::from_be_bytes(bytes.try_into()?);
        visitor.visit_i64(i64)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let byte = self.take_byte()?;
        visitor.visit_u8(byte)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let bytes = self.take_bytes(2)?;
        let u16 = u16::from_be_bytes(bytes.try_into()?);
        visitor.visit_u16(u16)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let bytes = self.take_bytes(4)?;
        let u32 = u32::from_be_bytes(bytes.try_into()?);
        visitor.visit_u32(u32)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let bytes = self.take_bytes(8)?;
        let u64 = u64::from_be_bytes(bytes.try_into()?);
        visitor.visit_u64(u64)
    }

    fn deserialize_f32<V>(self, _: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_f64<V>(self, _: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!();
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let bytes = self.decode_byte_slice()?;
        visitor.visit_str(std::str::from_utf8(&bytes)?)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let bytes = self.decode_byte_slice()?;
        visitor.visit_string(std::str::from_utf8(&bytes)?.to_string())
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let bytes = self.decode_byte_slice()?;
        visitor.visit_bytes(&bytes)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let bytes = self.decode_byte_slice()?;
        visitor.visit_byte_buf(bytes)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let byte = self.take_byte()?;
        match byte {
            0x00 => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(self, _: &'static str, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V>(self, _: &'static str, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        struct Access<'de, 'a> {
            de: &'a mut Deserializer<'de>,
            len: usize,
        }

        impl<'de> de::SeqAccess<'de> for Access<'de, '_> {
            type Error = Error;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
            where
                T: de::DeserializeSeed<'de>,
            {
                if self.len > 0 {
                    self.len -= 1;
                    let value = seed.deserialize(&mut *self.de)?;
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }
        }

        visitor.visit_seq(Access { de: self, len })
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V>(self, _: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_tuple(fields.len(), visitor)
    }

    fn deserialize_enum<V>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_ignored_any<V>(self, _: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }
}

impl<'de> de::EnumAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: de::DeserializeSeed<'de>,
    {
        let idx: u32 = self.take_byte()? as u32;
        let val: Result<_> = seed.deserialize(idx.into_deserializer());
        Ok((val?, self))
    }
}

impl<'de> de::VariantAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: de::DeserializeSeed<'de>,
    {
        seed.deserialize(self)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        de::Deserializer::deserialize_tuple(self, len, visitor)
    }

    fn struct_variant<V>(self, fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        de::Deserializer::deserialize_tuple(self, fields.len(), visitor)
    }
}

#[cfg(test)]
mod tests {
    use paste::paste;
    use serde::Deserialize;
    use serde::Serialize;
    use serde_bytes::ByteBuf;

    use super::*;

    type Index = u64;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Keyv {
        key: String,
        version: Option<u8>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum Key {
        Unit,
        NewType0(Index),
        NewType1(String),
        Struct(Keyv),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct MyStruct {
        a: u8,
        b: String,
        k1: Keyv,
        k2: Keyv,
        c: u8,
        k3: Key,
        k4: Option<Keyv>,
    }

    fn hex(input: &[u8]) -> String {
        let ans = input.iter().map(|b| format!("{:02x}", b)).collect::<String>();
        format!("0x{ans}")
    }

    macro_rules! test_codec {
        ( $($name:ident: $input:expr => $expect:expr),* ) => {
            paste!{
            $(
                #[test]
                fn [< test_ $name >]() -> Result<()> {
                    let mut input = $input;
                    let expect = $expect;
                    let expect = expect.replace(" ", "");
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
        bool_false: false => "0x00",
        bool_true: true => "0x01",

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

        // bytes codec
        bytes: ByteBuf::from(vec![0x01u8, 0xffu8]) => "0x01ff 0000",
        bytes_empty: ByteBuf::from(vec![]) => "0x0000",
        bytes_escape: ByteBuf::from(vec![0x00, 0x01, 0x02]) => "0x00ff 01 02 0000",

        // string codec
        string: "foo".to_string() => "0x666f6f 0000",
        string_empty: "".to_string() => "0x0000",
        string_utf8: "👋".to_string() => "0xf09f918b 0000",

        // enum codec
        enum_unit: Key::Unit => "0x00",
        enum_newtype0: Key::NewType0(0u64 as Index) => "0x010000000000000000",
        enum_newtype1: Key::NewType1("foo".to_string()) => "0x02666f6f0000",
        enum_struct: Key::Struct(Keyv { key: "foo".to_string(), version: Some(0x01u8) }) => "0x03666f6f0000 0101",

        // struct codec
        struct_keyv: Keyv { key: "foo".to_string(), version: Some(0x01u8) } => "0x666f6f0000 0101",
        struct_mystruct: MyStruct{
            a: 0xaa,
            b: "foo".to_string(),
            k1: Keyv { key: "foo".to_string(), version: Some(0xbbu8) },
            k2: Keyv { key: "bar".to_string(), version: None },
            c: 0xdd,
            k3: Key::NewType1("foo".to_string()),
            k4: Some(Keyv { key: "bar".to_string(), version: Some(0xeeu8) }),
        } => "0xaa 666f6f0000 666f6f0000 01bb 6261720000 00 dd 02666f6f0000 016261720000 01ee",
    }
}
