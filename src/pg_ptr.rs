#[derive(Debug, Clone, Copy)]
pub struct PgPtr(u128);

// | checksum 8 bytes | block_size_shift 1 byte | page_type 1 byte | block_offset 6 bytes |
// block size is (4096 << block_size_shift)
// Offset in file is 4096 * block_offset
impl PgPtr {
    pub fn new(checksum: u64, block_size_shift: u8, page_type: u8, block_offset: u64) -> PgPtr {
        Self(
            (checksum as u128)
                | ((block_size_shift as u128) << 64)
                | ((page_type as u128) << 72)
                | ((block_offset as u128) << 80),
        )
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(u128::from_le_bytes(
            bytes.try_into().expect("pg_ptr  incorrect length"),
        ))
    }

    pub fn get_bytes(&self) -> [u8; 16] {
        self.0.to_le_bytes()
    }

    /// Extracts bits 0 to 63 (64 bits)
    pub fn block_checksum(&self) -> u64 {
        (self.0 & 0xFFFFFFFFFFFFFFFF) as u64
    }

    /// Extracts bits 64 to 71 (8 bits)
    pub fn block_size_shift(&self) -> u8 {
        ((self.0 >> 64) & 0xFF) as u8
    }

    /// Extracts bits 72 to 79 (8 bits)
    pub fn page_type(&self) -> u8 {
        ((self.0 >> 72) & 0xFF) as u8
    }

    /// Extracts bits 80 to 127 (48 bits)
    pub fn block_offset(&self) -> u64 {
        ((self.0 >> 80) & 0xFFFFFFFFFFFF) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_ptr_create_serialise() {
        let pg_ptr = PgPtr::new(345u64, 0u8, 1u8, 45u64);
        let deser_pg_ptr = PgPtr::from_bytes(&pg_ptr.get_bytes());
        assert_eq!(345u64, deser_pg_ptr.block_checksum());
        assert_eq!(0u8, deser_pg_ptr.block_size_shift());
        assert_eq!(1u8, deser_pg_ptr.page_type());
        assert_eq!(45u64, deser_pg_ptr.block_offset());
    }
}
