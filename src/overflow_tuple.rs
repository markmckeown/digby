use crate::tuple::Overflow;
use crate::tuple::TupleTrait;
use crate::version_holder::VersionHolder;

// There must be some clever way to do this rather than copying code.
// The only difference is that the tuple is serialised with u32 for
// the key and value length rather than the u32 used in Tuple.
#[derive(Clone)]
pub struct OverflowTuple {
    serialized: Vec<u8>,
}

impl TupleTrait for OverflowTuple {
    fn get_key(&self) -> &[u8] {
        let key_len: usize = u32::from_le_bytes(self.serialized[0..4].try_into().unwrap()) as usize;
        &self.serialized[16..16 + key_len]
    }

    fn get_value(&self) -> &[u8] {
        let key_len: usize = u32::from_le_bytes(self.serialized[0..4].try_into().unwrap()) as usize;
        &self.serialized[16 + key_len..]
    }

    fn get_version(&self) -> u64 {
        VersionHolder::from_bytes(self.serialized[8..8 + 8].to_vec()).get_version()
    }

    fn get_serialized(&self) -> &[u8] {
        &self.serialized
    }

    fn get_byte_size(&self) -> usize {
        self.serialized.len()
    }

    fn get_overflow(&self) -> Overflow {
        Overflow::try_from(
            VersionHolder::from_bytes(self.serialized[8..8 + 8].to_vec()).get_flags(),
        )
        .unwrap()
    }
}

impl OverflowTuple {
    pub fn new(key: &[u8], value: &[u8], version: u64, overflow: Overflow) -> Self {
        assert!(
            key.len() < u32::MAX as usize,
            "Key size larger than u32 can hold."
        );
        assert!(
            value.len() < u32::MAX as usize,
            "Value size larger than u32 can hold."
        );
        let mut serialized = Vec::with_capacity(4 + key.len() + 4 + value.len() + 8);
        serialized.extend_from_slice(&(key.len() as u32).to_le_bytes());
        serialized.extend_from_slice(&(value.len() as u32).to_le_bytes());
        let version_holder = VersionHolder::new(overflow as u8, version);
        serialized.extend_from_slice(&version_holder.get_bytes()[0..8]);
        serialized.extend_from_slice(key);
        serialized.extend_from_slice(value);

        OverflowTuple { serialized }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        OverflowTuple { serialized: bytes }
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

        let tuple = OverflowTuple::new(&key, &value, version, Overflow::None);
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

        let tuple = OverflowTuple::new(&key, &value, version, Overflow::None);
        let deserialized = OverflowTuple::from_bytes(tuple.get_serialized().to_vec());

        assert_eq!(deserialized.get_key(), &key);
        assert_eq!(deserialized.get_value(), &value);
        assert_eq!(deserialized.get_version(), version);
    }
}
