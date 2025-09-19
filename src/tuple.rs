use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Cursor, Read}; 

pub struct Tuple {
    key: Vec<u8>,
    value: Vec<u8>,
    version: u64,
    serialized: Vec<u8>,
} 

impl Tuple {
    pub fn new(key: Vec<u8>, value: Vec<u8>, version: u64) -> Self {
        let mut serialized = Vec::new();
        serialized.extend_from_slice(&(key.len() as u32).to_le_bytes());
        serialized.extend_from_slice(&(value.len() as u32).to_le_bytes());
        serialized.extend_from_slice(&version.to_le_bytes());
        serialized.extend_from_slice(&key);
        serialized.extend_from_slice(&value);

        Tuple {
            key,
            value,
            version,
            serialized,
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let mut cursor = Cursor::new(&bytes[..]);
        let key_len = cursor.read_u32::<LittleEndian>().unwrap() as usize;
        let value_len = cursor.read_u32::<LittleEndian>().unwrap() as usize;
        let version = cursor.read_u64::<LittleEndian>().unwrap();

        let mut key = vec![0u8; key_len];
        cursor.read_exact(&mut key).unwrap();

        let mut value = vec![0u8; value_len];
        cursor.read_exact(&mut value).unwrap();

        Tuple {
            key,
            value,
            version,
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