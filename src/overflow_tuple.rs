use crate::tuple::Overflow;
use crate::tuple::TupleTrait;
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Cursor, Read};
use crate::version_holder::VersionHolder; 



// There must be some clever way to do this rather than copying code. 
// The only difference is that the tuple is serialised with u32 for
// the key and value length rather than the u32 used in Tuple.
#[derive(Clone)]
pub struct OverflowTuple {
    key: Vec<u8>,
    value: Vec<u8>,
    version: u64,
    overflow: Overflow,
    serialized: Vec<u8>,
} 

impl TupleTrait for OverflowTuple {
    fn get_key(&self) -> &[u8] {
        &self.key
    }

    fn get_value(&self) -> &[u8] {
        &self.value
    }

    fn get_version(&self) -> u64 {
        self.version
    }

    fn get_serialized(&self) -> &[u8] {
        &self.serialized
    }

    fn get_byte_size(&self) -> usize {
        self.serialized.len()
    }

    fn get_overflow(&self) -> &Overflow {
        &self.overflow
    }
}


impl OverflowTuple {
    pub fn new(key: &Vec<u8>, value: &Vec<u8>, version: u64, overflow: Overflow) -> Self {
        assert!(key.len() < u32::MAX as usize, "Key size larger than u32 can hold.");
        assert!(value.len() < u32::MAX as usize, "Value size larger than u32 can hold.");
        assert!(overflow != Overflow::None, "Cannot create a OverflowTuple when its not an Overflow.");
        let mut serialized = Vec::new();
        serialized.extend_from_slice(&(key.len() as u32).to_le_bytes());
        serialized.extend_from_slice(&(value.len() as u32).to_le_bytes());
        let version_holder = VersionHolder::new(overflow as u8, version);
        serialized.extend_from_slice(&version_holder.get_bytes()[0..8]);
        serialized.extend_from_slice(&key);
        serialized.extend_from_slice(&value);

        OverflowTuple {
            // TODO - these are duplicated in the serialized version, drop them and extract from
            // serialised version
            key: key.to_vec(),
            value: value.to_vec(),
            version,
            overflow,
            serialized,
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let mut cursor = Cursor::new(&bytes[..]);
        let key_len = cursor.read_u32::<LittleEndian>().unwrap() as usize;
        let value_len = cursor.read_u32::<LittleEndian>().unwrap() as usize;    
        
        let mut version_bytes: [u8; 8] = [0u8; 8];
        cursor.read_exact(&mut version_bytes).unwrap();
        let version_holder = VersionHolder::from_bytes(version_bytes.to_vec());
        let overflow = Overflow::try_from(version_holder.get_flags()).unwrap();
        assert!(overflow != Overflow::None);
        
        let mut key = vec![0u8; key_len];
        cursor.read_exact(&mut key).unwrap();

        let mut value = vec![0u8; value_len];
        cursor.read_exact(&mut value).unwrap();

        OverflowTuple {
            key,
            value,
            version: version_holder.get_version(),
            overflow: overflow,
            serialized: bytes,
        }
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

        let tuple = OverflowTuple::new(&key.clone(), &value.clone(), version, Overflow::ValueOverflow);
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

        let tuple = OverflowTuple::new(&key.clone(), &value.clone(), version, Overflow::ValueOverflow);
        let deserialized = OverflowTuple::from_bytes(tuple.get_serialized().to_vec());

        assert_eq!(deserialized.get_key(), &key);
        assert_eq!(deserialized.get_value(), &value);
        assert_eq!(deserialized.get_version(), version);
    }
}
