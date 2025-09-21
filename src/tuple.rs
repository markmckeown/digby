use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Cursor, Read}; 


// A tuple has to fit inside a data page - other wise it needs to be
// stored in an overflow page or series of overflow pages. 
// The simplest approach would be to have a single tuple for
// an overflow page or series of pages and avoid packing the
// overflow pages.
// So if a tuple overflows we store as the value the page number
// of the overflow page. The overflow page can have links
// to other overflow pages.
//
// There is a challenge if the key on its own would overflow
// a data page. To address this we could use a SHA256 of the
// the key. So if a key is over some size we lookup the 
// SHA256 of the key. We can then go to the overdlow page
// to get the full key.
//
// This means there are a number of cases to deal with:
// 1. Key and value fit in data page - store as normal
// 2. Key fits in data page, value overflows - store key and page number
//    of overflow page as value
// 3. Key overflows, value fits in data page - store SHA256 of key
//    and page number of overflow page as value
// 4. Key and value overflow - store SHA256 of key and page number
//    of overflow page as value.
//
// When we come to store a tuple we know which Overflow type it is.
// When we want to look up a tuple given the key we know whether 
// the key would overflow and can use the SHA256 of the key. 
// When we get the tuple we can from the overflow pages we
// can get the full key and check.
//
// We cannot handle different keys with the same SHA256 - but we
// can detect this clash and crash.

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum Overflow {
    None = 0,
    ValueOverflow = 1,
    KeyOverflow = 2,
    KeyValueOverflow = 3,
}

impl TryFrom<u8> for Overflow {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Overflow::None),
            1 => Ok(Overflow::ValueOverflow),
            2 => Ok(Overflow::KeyOverflow),
            3 => Ok(Overflow::KeyValueOverflow),
            _ => Err(()),
        }
    }
}




pub struct Tuple {
    key: Vec<u8>,
    value: Vec<u8>,
    version: u64,
    overflow: Overflow,
    serialized: Vec<u8>,
} 

impl Tuple {
    pub fn new(key: Vec<u8>, value: Vec<u8>, version: u64) -> Self {
        let mut serialized = Vec::new();
        serialized.extend_from_slice(&(key.len() as u32).to_le_bytes());
        serialized.extend_from_slice(&(value.len() as u32).to_le_bytes());
        serialized.extend_from_slice(&version.to_le_bytes());
        serialized.push(Overflow::None as u8); // Overflow byte
        serialized.extend_from_slice(&key);
        serialized.extend_from_slice(&value);

        Tuple {
            key,
            value,
            version,
            overflow: Overflow::None,
            serialized,
        }
    }

    pub fn new_with_overflow(key: Vec<u8>, value: Vec<u8>, version: u64, overflow: Overflow) -> Self {
        let mut serialized = Vec::new();
        serialized.extend_from_slice(&(key.len() as u32).to_le_bytes());
        serialized.extend_from_slice(&(value.len() as u32).to_le_bytes());
        serialized.extend_from_slice(&version.to_le_bytes());
        serialized.push(overflow as u8); // Overflow byte
        serialized.extend_from_slice(&key);
        serialized.extend_from_slice(&value);

        Tuple {
            key,
            value,
            version,
            overflow,
            serialized,
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let mut cursor = Cursor::new(&bytes[..]);
        let key_len = cursor.read_u32::<LittleEndian>().unwrap() as usize;
        let value_len = cursor.read_u32::<LittleEndian>().unwrap() as usize;
        let version = cursor.read_u64::<LittleEndian>().unwrap();
        let overflow_byte = cursor.read_u8().unwrap();
        let overflow = Overflow::try_from(overflow_byte).unwrap();

        let mut key = vec![0u8; key_len];
        cursor.read_exact(&mut key).unwrap();

        let mut value = vec![0u8; value_len];
        cursor.read_exact(&mut value).unwrap();

        Tuple {
            key,
            value,
            version,
            overflow,
            serialized: bytes,
        }
    }

    pub fn get_key(&self) -> &[u8] {
        &self.key
    }

    pub fn get_value(&self) -> &[u8] {
        &self.value
    }

    pub fn get_version(&self) -> u64 {
        self.version
    }

    pub fn get_serialized(&self) -> &[u8] {
        &self.serialized
    }

    pub fn get_size(&self) -> usize {
        self.serialized.len()
    }

    pub fn get_overflow(&self) -> &Overflow {
        &self.overflow
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple() {
        let key = b"key".to_vec();
        let value = b"value".to_vec();
        let version = 1;

        let tuple = Tuple::new(key.clone(), value.clone(), version);
        assert_eq!(tuple.get_key(), &key);
        assert_eq!(tuple.get_value(), &value);
        assert_eq!(tuple.get_version(), version);
        assert_eq!(tuple.get_serialized(), &tuple.serialized);
    }

    #[test]
    fn test_tuple_from_bytes() {
        let key = b"key".to_vec();
        let value = b"value".to_vec();
        let version = 1;

        let tuple = Tuple::new(key.clone(), value.clone(), version);
        let deserialized = Tuple::from_bytes(tuple.get_serialized().to_vec());

        assert_eq!(deserialized.get_key(), &key);
        assert_eq!(deserialized.get_value(), &value);
        assert_eq!(deserialized.get_version(), version);
    }
}