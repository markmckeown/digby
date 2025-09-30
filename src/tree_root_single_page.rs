use crate::tree_leaf_page::TreeLeafPage;
use crate::page::PageTrait;
use crate::page::Page;
use crate::page::PageType;
use crate::tuple::Tuple;


// If there is only a single node, the root node, in the B-tree then
// it will store key-value pairs. We give this page node a special
// type, but it is just a wrapped TreeLeafPage.
pub struct TreeRootSinglePage {
    page: TreeLeafPage
}

impl PageTrait for TreeRootSinglePage {
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

impl TreeRootSinglePage {
pub fn new(page_size: u64, page_number: u32, version: u64) -> Self {
        let mut page = TreeLeafPage::new(page_size, page_number);
        page.make_tree_root_single_page();
        page.set_version(version);
        TreeRootSinglePage { page }
    }


    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let page = Page::from_bytes(bytes);
        return Self::from_page(page);
    }

    // Create a DataPage from a Page - read bytes from disk,
    // determine it is a DataPage, and wrap it.
    pub fn from_page(page: Page) -> Self {
        if page.get_type() != PageType::TreeRootSingle {
            panic!("Page type is not TreeRootSingle");
        }
        let tree_leaf_page = TreeLeafPage::from_page(page);
        TreeRootSinglePage { 
            page: tree_leaf_page
         }
    }

    pub fn can_fit(&mut self, size: usize) -> bool {
        self.page.can_fit(size)
    }

    pub fn get_all_tuples(&mut self, page_size: usize) -> Vec<Tuple> {
        self.page.get_all_tuples(page_size)
    }

    pub fn get_tuple(&self, key: Vec<u8>, page_size: usize) -> Option<Tuple> {
        self.page.get_tuple(key, page_size)
    }

    pub fn store_tuple(&mut self, new_tuple: Tuple, page_size: usize) -> () {
        self.page.store_tuple(new_tuple, page_size);
    }

}