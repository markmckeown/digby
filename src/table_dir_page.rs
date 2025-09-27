use crate::{page::{Page, PageTrait, PageType}, TableDirEntry, TreeLeafPage};


// TableDirPage is just a TreeLeafPage wrapped.
pub struct TableDirPage {
    page : TreeLeafPage
}

impl PageTrait for TableDirPage {
    fn get_bytes(&self) -> &[u8] {
        self.page.get_bytes()
    }

    fn get_page_number(& self) -> u32 {
        self.page.get_page_number()
    }

    fn get_page(&mut self) -> &mut Page {
        self.page.get_page()       
    }

    fn get_version(& self) -> u64 {
        self.page.get_version()         
    }

    fn set_version(&mut self, version: u64) -> () {
        self.page.set_version(version);         
    }
}


impl TableDirPage {
    pub fn new(page_size: u64, page_number: u32, version: u64) -> Self {
        let mut page = TreeLeafPage::new(page_size, page_number);
        page.make_table_dir_page();
        page.set_version(version);
        TableDirPage { page }
    }


    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    // Create a DataPage from a Page - read bytes from disk,
    // determine it is a DataPage, and wrap it.
    pub fn from_page(mut page: Page) -> Self {
        if page.get_type() != PageType::TableDir {
            panic!("Page type is not TableDir");
        }
        let tree_leaf_page = TreeLeafPage::from_page(page);
        TableDirPage { 
            page: tree_leaf_page
         }
    }

    
    pub fn can_fit(&mut self, size: usize) -> bool {
        self.page.can_fit(size)
    }

    pub fn add_table_entry(&mut self, table_dir_entry: TableDirEntry, page_size: u64) {
        self.page.store_tuple(table_dir_entry.get_tuple().clone(), page_size as usize);
    }

    pub fn get_table_page(&mut self, name: Vec<u8>, page_size: u64) -> Option<u32> {
        let value = self.page.get_tuple(name, page_size as usize);
        if let Some(tuple) = value {
            Some(u32::from_le_bytes(tuple.get_value().try_into().unwrap()))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_basic() {
        let mut page = TableDirPage::new(4096, 45, 679);
        let table_dir_entry = TableDirEntry::new(b"mmk".to_vec(), 45, 678);
        assert!(page.can_fit(table_dir_entry.get_byte_size()));
        page.add_table_entry(table_dir_entry, 4096);
        assert!(page.get_table_page(b"mmk".to_vec(), 4096).unwrap() == 45);
    }
}