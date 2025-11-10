use crate::{block_layer::PageConfig, page::{Page, PageTrait, PageType}, TableDirEntry, TreeLeafPage};
use crate::tuple::TupleTrait;


// TableDirPage is just a TreeLeafPage wrapped.
pub struct TableDirPage {
    page : TreeLeafPage
}

impl PageTrait for TableDirPage {
    fn get_page_bytes(&self) -> &[u8] {
        self.page.get_page_bytes()
    }

    fn get_page_number(& self) -> u32 {
        self.page.get_page_number()
    }

    fn set_page_number(&mut self,  page_no: u32) -> () {
        self.page.set_page_number(page_no)
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
    pub fn create_new(page_config: &PageConfig, page_number: u32, version: u64) -> Self {
        let mut page = TreeLeafPage::create_new(page_config, page_number);
        page.make_table_dir_page();
        page.set_version(version);
        TableDirPage { page }
    }

    // Create a DataPage from a Page - read bytes from disk,
    // determine it is a DataPage, and wrap it.
    pub fn from_page(page: Page) -> Self {
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

    pub fn add_table_entry(&mut self, table_dir_entry: TableDirEntry) {
        self.page.store_tuple(table_dir_entry.get_tuple().clone());
    }

    pub fn get_table_page(&mut self, name: Vec<u8>) -> Option<u32> {
        let value = self.page.get_tuple(&name);
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
        let page_config = PageConfig{block_size: 4096, page_size: 4092};
        let mut page = TableDirPage::create_new(&page_config, 45, 679);
        let table_dir_entry = TableDirEntry::new(b"mmk".to_vec(), 45, 678);
        assert!(page.can_fit(table_dir_entry.get_byte_size()));
        page.add_table_entry(table_dir_entry);
        assert!(page.get_table_page(b"mmk".to_vec()).unwrap() == 45);
    }
}