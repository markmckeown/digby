#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq)]
pub struct PageNo(pub u64);

// PageNo:
//
// | pg_ctr_blk_cnt_exp (1 byte) | file_blk_offset 7 bytes |
//
// A Page Container is made up of one or more blocks. The number
// if blocks is encoded as pg_ctr_blk_cnt_exp in the page number.
// The number of blocks in the page container is
//
//   2 << pg_ctr_blk_cnt_exp
//
// So if pg_ctr_blk_cnt_exp is zero the page container contains
// one block. pg_ctr_blk_cnt_exp is limited to 8, which limits
// the size of a page container to 256 blocks.
//
// The file_blk_offset is the offset into the file where the
// page container starts - the offset is in blocks.
//
// Note there is no hard coded block size.
impl PageNo {
    const TOP_BYTE_MASK: u64 = 0xFF00_0000_0000_0000;
    const BOTTOM_56_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;

    pub fn new(pg_blk_cnt_exp: u8, pg_blk_offset: u64) -> Self {
        assert!(pg_blk_cnt_exp <= 8);
        Self((u64::from(pg_blk_cnt_exp) << 56) | (pg_blk_offset & Self::BOTTOM_56_MASK))
    }

    pub fn from_u64(page_no: u64) -> Self {
        Self(page_no)
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(u64::from_le_bytes(
            bytes.try_into().expect("slice with incorrect length"),
        ))
    }

    pub fn to_u64(&self) -> u64 {
        self.0
    }

    pub fn get_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    pub fn get_blk_cnt(&self) -> u64 {
        1 << (self.0 >> 56)
    }

    pub fn get_pg_blk_size(&self, block_size: usize) -> usize {
        block_size * self.get_blk_cnt() as usize
    }

    pub fn set_blk_offset(&mut self, file_blk_offset: u64) {
        self.0 = (self.0 & Self::TOP_BYTE_MASK) | (file_blk_offset & Self::BOTTOM_56_MASK);
    }

    pub fn get_blk_offset(&self) -> u64 {
        self.0 & Self::BOTTOM_56_MASK
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_no() {
        let mut page_no = PageNo::from_u64(0);
        assert_eq!(page_no.get_blk_cnt(), 1);
        assert_eq!(page_no.get_pg_blk_size(4096), 4096);
        assert_eq!(page_no.get_blk_offset(), 0);

        page_no.set_blk_offset(34);
        assert_eq!(page_no.get_blk_cnt(), 1);
        assert_eq!(page_no.get_blk_offset(), 34);
        assert_eq!(page_no.get_bytes(), [34, 0, 0, 0, 0, 0, 0, 0]);

        let page_no_2 = PageNo::new(1, 57);
        assert_eq!(page_no_2.get_blk_cnt(), 2);
        assert_eq!(page_no_2.get_pg_blk_size(4096), 4096 * 2);
        assert_eq!(page_no_2.get_blk_offset(), 57);
    }
}
