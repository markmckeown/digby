pub struct PageNo(u64);

impl PageNo {
    const TOP_BYTE_MASK: u64 = 0xFF00_0000_0000_0000;
    const BOTTOM_56_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;

    pub fn new(size: u32, offset: u64) -> Self {
        assert!(size % 4096 == 0);
        assert!(size <= 4096 * 256);
        Self((u64::from((size/4096 - 1) as u32) << 56)| (offset & Self::BOTTOM_56_MASK))
    }

    pub fn from(page_no: u64) -> Self {
        Self(page_no)
    }


    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(u64::from_le_bytes(
            bytes.try_into().expect("slice with incorrect length"),
        ))
    }

    pub fn from_page_no(&self, new_offset: u64) -> Self {
        Self((self.0 & Self::TOP_BYTE_MASK) | (new_offset & Self::BOTTOM_56_MASK))
    }

    pub fn get_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    pub fn get_size(&self) -> usize {
        (((self.0 >> 56) as usize) + 1) * 4096
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
        let mut page_no = PageNo::from(0);
        assert_eq!(page_no.get_size(), 4096);
        assert_eq!(page_no.get_offset(), 0);

        page_no.set_offset(34);
        assert_eq!(page_no.get_size(), 4096);
        assert_eq!(page_no.get_offset(), 34);
        assert_eq!(page_no.get_bytes(), [34, 0, 0, 0, 0, 0, 0, 0]);

        let jumbo_page = PageNo::new(1048576, 45);
        assert_eq!(jumbo_page.get_size(), 1048576);
        assert_eq!(jumbo_page.get_offset(), 45);

        let new_jumbo_page = jumbo_page.from_page_no(46);
        assert_eq!(new_jumbo_page.get_size(), 1048576);
        assert_eq!(new_jumbo_page.get_offset(), 46);
    }

}