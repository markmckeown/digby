pub struct PageNo(u64);

pub enum BlockSize {
    PageSize4K = 0,
    PageSize8K = 1,
    PageSize16K = 2,
    PageSize32K = 3,
    PageSize64K = 4,
    PageSize128K = 5,
    PageSize256K = 6,
    PageSize512K = 7,
    PageSize1M = 8,
}

impl TryFrom<u8> for BlockSize {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(BlockSize::PageSize4K),
            1 => Ok(BlockSize::PageSize8K),
            2 => Ok(BlockSize::PageSize16K),
            3 => Ok(BlockSize::PageSize32K),
            4 => Ok(BlockSize::PageSize64K),
            5 => Ok(BlockSize::PageSize128K),
            6 => Ok(BlockSize::PageSize256K),
            7 => Ok(BlockSize::PageSize512K),
            8 => Ok(BlockSize::PageSize1M),
            _ => Err(()),
        }
    }
}

impl BlockSize {
    pub fn get_byte_size(&self) -> usize {
        match self {
            BlockSize::PageSize4K => 4096,
            BlockSize::PageSize8K => 8192,
            BlockSize::PageSize16K => 16384,
            BlockSize::PageSize32K => 32768,
            BlockSize::PageSize64K => 65536,
            BlockSize::PageSize128K => 131072,
            BlockSize::PageSize256K => 262144,
            BlockSize::PageSize512K => 524288,
            BlockSize::PageSize1M => 1048576,
        }
    }
        
}


impl PageNo {
    const TOP_BYTE_MASK: u64 = 0xFF00_0000_0000_0000;
    const BOTTOM_56_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;

    pub fn new(size: BlockSize, offset: u64) -> Self {
        Self((u64::from(size as u8) << 56)| (offset & Self::BOTTOM_56_MASK))
    }

    pub fn from_u64(page_no: u64) -> Self {
        Self(page_no)
    }



    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(u64::from_le_bytes(
            bytes.try_into().expect("slice with incorrect length"),
        ))
    }

    pub fn get_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    pub fn get_size(&self) -> BlockSize {
        BlockSize::try_from((self.0 >> 56) as u8).unwrap()
    }

    pub fn set_offset(&mut self, offset: u64) {
        self.0 = (self.0 & Self::TOP_BYTE_MASK) | (offset & Self::BOTTOM_56_MASK);
    }

    pub fn get_offset(&self) -> u64 {
        self.0 & Self::BOTTOM_56_MASK
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_no() { 
        let mut page_no = PageNo::from_u64(0);
        assert_eq!(page_no.get_size().get_byte_size(), 4096);
        assert_eq!(page_no.get_offset(), 0);

        page_no.set_offset(34);
        assert_eq!(page_no.get_size().get_byte_size(), 4096);
        assert_eq!(page_no.get_offset(), 34);
        assert_eq!(page_no.get_bytes(), [34, 0, 0, 0, 0, 0, 0, 0]);

        let mut jumbo_page = PageNo::new(BlockSize::PageSize1M, 45);
        assert_eq!(jumbo_page.get_size().get_byte_size(), 1048576);
        assert_eq!(jumbo_page.get_offset(), 45);

        jumbo_page.set_offset(46);
        assert_eq!(jumbo_page.get_size().get_byte_size(), 1048576);
        assert_eq!(jumbo_page.get_offset(), 46);
    }

}