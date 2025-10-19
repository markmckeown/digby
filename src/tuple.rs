use crate::version_holder::VersionHolder; 


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
    ValueCompressed = 4,
    KeyValueCompressed = 5,
}

impl TryFrom<u8> for Overflow {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Overflow::None),
            1 => Ok(Overflow::ValueOverflow),
            2 => Ok(Overflow::KeyOverflow),
            3 => Ok(Overflow::KeyValueOverflow),
            4 => Ok(Overflow::ValueCompressed),
            5 => Ok(Overflow::KeyValueCompressed),
            _ => Err(()),
        }
    }
}


pub trait  TupleTrait {
    fn get_key(&self) -> &[u8];
    fn get_value(&self) -> &[u8];
    fn get_version(&self) -> u64;
    fn get_serialized(&self) -> &[u8];
    fn get_byte_size(&self) -> usize;
    fn get_overflow(&self) -> Overflow;
}



#[derive(Clone)]
pub struct Tuple {
    serialized: Vec<u8>,
} 

impl TupleTrait for Tuple {
    fn get_key(&self) -> &[u8] {
        let key_len = u16::from_le_bytes(self.serialized[0 .. 2].try_into().unwrap()) as usize;
        &self.serialized[12 .. 12 + key_len]
    }

    fn get_value(&self) -> &[u8] {
        let key_len = u16::from_le_bytes(self.serialized[0 .. 2].try_into().unwrap()) as usize;
        &self.serialized[12 + key_len ..]
    }

    fn get_version(&self) -> u64 {
        VersionHolder::from_bytes(self.serialized[4 .. 4 + 8].to_vec()).get_version()
    }

    fn get_serialized(&self) -> &[u8] {
        &self.serialized
    }

    fn get_byte_size(&self) -> usize {
        self.serialized.len()
    }

    fn get_overflow(&self) -> Overflow {
        Overflow::try_from(VersionHolder::from_bytes(self.serialized[4 .. 4 + 8].to_vec()).get_flags()).unwrap()
    }

}

impl Tuple {
    pub fn new(key: &Vec<u8>, value: &Vec<u8>, version: u64) -> Self {
        assert!(key.len() < u16::MAX as usize, "Key size larger than u16 can hold.");
        assert!(value.len() < u16::MAX as usize, "Value size larger than u16 can hold.");
        let mut serialized = Vec::with_capacity(2 + key.len() + 2 + value.len() + 8);
        serialized.extend_from_slice(&(key.len() as u16).to_le_bytes());
        serialized.extend_from_slice(&(value.len() as u16).to_le_bytes());
        let version_holder = VersionHolder::new(0, version);
        serialized.extend_from_slice(&version_holder.get_bytes()[0..8]);
        serialized.extend_from_slice(&key);
        serialized.extend_from_slice(&value);
        Tuple {
            serialized,
        }
    }

    pub fn new_with_overflow(key: &Vec<u8>, value: &Vec<u8>, version: u64, overflow: Overflow) -> Self {
        assert!(key.len() < u16::MAX as usize, "Key size larger than u16 can hold.");
        assert!(value.len() < u16::MAX as usize, "Value size larger than u16 can hold.");
        let mut serialized = Vec::with_capacity(2 + key.len() + 2 + value.len() + 8);
        serialized.extend_from_slice(&(key.len() as u16).to_le_bytes());
        serialized.extend_from_slice(&(value.len() as u16).to_le_bytes());
        let version_holder = VersionHolder::new(overflow as u8, version);
        serialized.extend_from_slice(&version_holder.get_bytes()[0..8]);
        serialized.extend_from_slice(&key);
        serialized.extend_from_slice(&value);
        Tuple {
            serialized,
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Tuple {
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

        let tuple = Tuple::new(&key.clone(), &value.clone(), version);
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

        let tuple = Tuple::new(&key.clone(), &value.clone(), version);
        let deserialized = Tuple::from_bytes(tuple.get_serialized().to_vec());

        assert_eq!(deserialized.get_key(), &key);
        assert_eq!(deserialized.get_value(), &value);
        assert_eq!(deserialized.get_version(), version);
    }
}