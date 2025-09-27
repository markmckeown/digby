use crate::page::Page;
use crate::page::PageTrait;

// Header 30 bytes.
// | Checksum(u32)   | Page No (u32) | Version (u64) | Type(u8) | Reserved(1 bytes) | Entries (u16) |
// | FreeSpace (u16) | ParentPage (u32) | EndLeafPage (u32) |
//
// | TreeDirEntry | TreeDirEntry ...|
//
// | IndexEntry | IndexEntry |
pub struct TreeInternalPage {
    page: Page
}

impl PageTrait for TreeInternalPage {
    fn get_bytes(&self) -> &[u8] {
        self.page.get_bytes()
    }

    fn get_page_number(& self) -> u32 {
        self.page.get_page_number()
    }

    fn get_page(&mut self) -> &mut Page {
        &mut self.page
    }

    fn get_version(& self) -> u64 {
        self.page.get_version()     
    }

    fn set_version(&mut self, version: u64) -> () {
        self.page.set_version(version);   
    }
}

impl TreeInternalPage {
    pub fn new(page_size: u64, page_number: u32, version: u64) -> Self {
        let mut tree_page_dir =  TreeInternalPage {
            page: Page::new(page_size),
        };
        tree_page_dir.page.set_type(crate::page::PageType::TreeInternal);
        tree_page_dir.page.set_page_number(page_number);
        tree_page_dir.set_version(version);
        tree_page_dir
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    pub fn from_page(mut page: Page) -> Self {
        if page.get_type() != crate::page::PageType::TreeInternal {
            panic!("Invalid page type for TreePageDir");
        }

        let tree_page_dir = TreeInternalPage { page };
        tree_page_dir
    }

    pub fn get_entries(&self) -> u16 {
        let index = 18;
        let slice = &self.page.get_bytes()[index..index + 2];
        let array: [u8; 2] = slice.try_into().unwrap();
        u16::from_le_bytes(array)
    }

    pub fn set_entries(&mut self, entries: u16) -> () {
        let index = 18;
        self.page.get_bytes_mut()[index..index+2].copy_from_slice(&entries.to_le_bytes());
    }

    pub fn get_free_space(&self) -> u16 {
        let index = 20;
        let slice = &self.page.get_bytes()[index..index + 2];
        let array: [u8; 2] = slice.try_into().unwrap();
        u16::from_le_bytes(array)
    }

    pub fn set_free_space(&mut self, entries: u16) -> () {
        let index = 20;
        self.page.get_bytes_mut()[index..index+2].copy_from_slice(&entries.to_le_bytes());
    }

    pub fn get_parent_page(&self) -> u32 {
        let index = 22;
        let slice = &self.page.get_bytes()[index..index + 4];
        let array: [u8; 4] = slice.try_into().unwrap();
        u32::from_le_bytes(array)
    }

    pub fn set_parent_page(&mut self, page_no: u32) -> () {
        let index = 22;
        self.page.get_bytes_mut()[index..index + 4].copy_from_slice(&page_no.to_le_bytes());
    }


    pub fn get_end_leaf_page(&self) -> u32 {
        let index = 26;
        let slice = &self.page.get_bytes()[index..index + 4];
        let array: [u8; 4] = slice.try_into().unwrap();
        u32::from_le_bytes(array)
    }

    pub fn set_end_leaf_page(&mut self, page_no: u32) -> () {
        let index = 26;
        self.page.get_bytes_mut()[index..index + 4].copy_from_slice(&page_no.to_le_bytes());
    }

}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_set_entries() {
        let mut page = TreeInternalPage::new(4096, 45, 567);
        
        assert!(0 == page.get_entries());
        page.set_entries(79);
        assert!(79 == page.get_entries());
        
        assert!(0 == page.get_free_space());
        page.set_free_space(179);
        assert!(179 == page.get_free_space());
        
        assert!(0 == page.get_parent_page());
        page.set_parent_page(2179);
        assert!(2179 == page.get_parent_page());

        assert!(0 == page.get_end_leaf_page());
        page.set_end_leaf_page(32179);
        assert!(32179 == page.get_end_leaf_page());

    }
}