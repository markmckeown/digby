pub struct VersionHolder(u64);

impl VersionHolder {
    const TOP_BYTE_MASK: u64 = 0xFF00_0000_0000_0000;
    const BOTTOM_56_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;

    pub fn new(flags: u8, version: u64) -> Self {
        Self((u64::from(flags) << 56) | (version & Self::BOTTOM_56_MASK))
    }


    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(u64::from_le_bytes(
            bytes
                .try_into()
                .expect("slice with incorrect length"),
        ))
    }

    pub fn get_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    pub fn get_flags(&self) -> u8 {
        (self.0 >> 56) as u8
    }

    pub fn set_flags(&mut self, flags: u8) {
        self.0 = (self.0 & Self::BOTTOM_56_MASK) | (u64::from(flags) << 56);
    }

    pub fn set_version(&mut self, version: u64) {
        self.0 = (self.0 & Self::TOP_BYTE_MASK) | (version & Self::BOTTOM_56_MASK);
    }

    pub fn get_version(&self) -> u64 {
        self.0 & Self::BOTTOM_56_MASK
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
        const MAX_IN_7_BYTES: u64 = (1 << 56) - 1; // This is 2^56 - 1
        let version = MAX_IN_7_BYTES - 1;
        let mut version_holder = VersionHolder::new(7, version);
        assert_eq!(7, version_holder.get_flags());
        assert_eq!(version, version_holder.get_version());

        version_holder.set_flags(92);
        version_holder.set_version(89);
        assert_eq!(92, version_holder.get_flags());
        assert_eq!(89, version_holder.get_version());
    }
}
