pub struct Page {
    page_number: Option<u64>,
    bytes: Vec<u8>,
}

impl Page {
    pub fn new(page_size: u64) -> Self {
        Page {
            page_number: None,
            bytes: vec![0u8; page_size as usize],
        }
    }

    pub fn from_bytes(page_number: u64, bytes: Vec<u8>) -> Self {
        Page {
            page_number: Some(page_number),
            bytes,
        }
    }

    pub fn get_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn get_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }

    pub fn get_page_number(&self) -> Option<u64> {
        self.page_number
    }

    pub fn set_page_number(&mut self, page_number: u64) {
        self.page_number = Some(page_number);
    }
}


impl Drop for Page {
    fn drop(&mut self) {
        // No special cleanup needed for Page
    }   
    
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let page = Page::new(4096);
        assert_eq!(page.get_bytes().len(), 4096);
        assert_eq!(page.get_page_number(), None);
    }

}
