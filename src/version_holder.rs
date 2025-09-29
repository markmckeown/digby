pub struct VersionHolder {
    bytes: [u8; 8]
}


impl VersionHolder {
    const MAX_IN_7_BYTES: u64 = (1 << 56) - 1; // This is 2^56 - 1

    pub fn new (flags: u8, version: u64) -> Self {
        assert!(version < VersionHolder::MAX_IN_7_BYTES, "Version is too larget to store in 7 bytes.");
        let mut bytes_8: [u8; 8] = version.to_le_bytes();
        bytes_8[7] = flags;
        VersionHolder { 
            bytes: bytes_8
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
       VersionHolder { 
            bytes: bytes[0..8].try_into().unwrap()
        } 

    }

    pub fn get_bytes(&self) -> Vec<u8> {
        self.bytes.to_vec()
    }

    pub fn get_flags(&self) -> u8 {
        self.bytes[7]
    }

    pub fn get_version(&self) -> u64 {
        let mut bytes_le_8 = [0u8; 8];
        bytes_le_8[0..7].copy_from_slice(&self.bytes[0..7]);
        u64::from_le_bytes(bytes_le_8)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_set() {
        let version_holder = VersionHolder::new(7, 345);
        assert!(7 == version_holder.get_flags());
        assert!(345 == version_holder.get_version());
    }

    #[test]
    fn test_get_set_large_version() {
        let version = VersionHolder::MAX_IN_7_BYTES - 1;
        let version_holder = VersionHolder::new(7, version);
        assert!(7 == version_holder.get_flags());
        assert!(version == version_holder.get_version());
    }
}