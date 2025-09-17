use crate::file_layer::FileLayer; 
use crate::page::Page; 
use crate::page::PageTrait;

pub struct BlockLayer {
    file_layer: FileLayer,
}

impl BlockLayer {
    pub fn new(file_layer: FileLayer) -> Self {
        BlockLayer { file_layer }
    }

    pub fn get_page(&mut self, page_number: u32, page_size: u64) -> Page {
        let mut page = Page::new(page_size);
        self.file_layer.read_page_from_disk(&mut page, page_number, page_size);
        page
    }

    pub fn put_page(&mut self, page: &mut Page, page_size: u64) {
        let page_number = page.get_page_number();
        self.file_layer.write_page_to_disk(page, page_number, page_size);
    }
}   