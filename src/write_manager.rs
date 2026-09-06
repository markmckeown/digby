use crate::file_layer::FileLayer;
use crate::page::Page;
use crate::page_no::PageNo;

pub struct WriteManager {
    primary: FileLayer,
    mirror: Option<FileLayer>,
}

impl WriteManager {
    pub fn new(primary: FileLayer) -> Self {
        WriteManager {
            primary,
            mirror: None,
        }
    }

    pub fn new_with_mirror(primary: FileLayer, mirror: FileLayer) -> Self {
        WriteManager {
            primary,
            mirror: Some(mirror),
        }
    }

    pub fn get_block_count(&self) -> u64 {
        self.primary.get_block_count()
    }

    pub fn append_new_page(&mut self, page: &Page, page_no: &PageNo) {
        if let Some(mirror) = &mut self.mirror {
            mirror.append_new_page(page, page_no);
        }
        self.primary.append_new_page(page, page_no);
    }

    pub fn write_page_to_disk(&mut self, page: &Page, page_no: &PageNo) -> std::io::Result<()> {
        if let Some(mirror) = &mut self.mirror {
            mirror.write_page_to_disk(page, page_no)?
        }
        self.primary.write_page_to_disk(page, page_no)
    }

    pub fn read_page_from_disk(
        &mut self,
        page: &mut Page,
        page_no: &PageNo,
    ) -> std::io::Result<()> {
        self.primary.read_page_from_disk(page, page_no)
    }

    pub fn read_page_from_mirror(
        &mut self,
        page: &mut Page,
        page_no: &PageNo,
    ) -> std::io::Result<()> {
        match &mut self.mirror {
            Some(mirror) => mirror.read_page_from_disk(page, page_no),
            None => panic!("Attempt to read page mirror when there is no mirror"),
        }
    }

    pub fn write_page_to_primary(&mut self, page: &Page, page_no: &PageNo) -> std::io::Result<()> {
        self.primary.write_page_to_disk(page, page_no)
    }

    pub fn sync_all(&mut self) {
        if let Some(mirror) = &mut self.mirror {
            mirror.sync_all()
        }
        self.primary.sync_all()
    }

    pub fn sync_data(&mut self) {
        if let Some(mirror) = &mut self.mirror {
            mirror.sync_data()
        }
        self.primary.sync_data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PageType;
    const BLOCK_SIZE: usize = 4096;
    use rand::RngExt;
    use rand::distr::Alphanumeric;
    use tempfile::tempfile;

    #[test]
    fn test_write_manager_write_and_read() {
        let primary_file = tempfile().expect("Failed to create temp file");
        let mirror_file = tempfile().expect("Failed to create temp file");
        let primary_layer = FileLayer::new(primary_file, BLOCK_SIZE);
        let mirror_layer = FileLayer::new(mirror_file, BLOCK_SIZE);
        let mut wrt_mgr = WriteManager::new_with_mirror(primary_layer, mirror_layer);
        let mut page = Page::new(BLOCK_SIZE, BLOCK_SIZE - 4); // Create a new page
        let page_no = PageNo::new(PageType::Null, 0, 0);
        wrt_mgr.append_new_page(&page, &page_no);
        let test_data: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(BLOCK_SIZE)
            .map(char::from)
            .collect();
        page.get_pg_ctr_bytes_mut()
            .copy_from_slice(test_data.as_bytes()); // Fill the page with test data

        // Write the page to disk
        wrt_mgr
            .write_page_to_disk(&page, &page_no)
            .expect("Failed to write page");

        // Read the page back from disk
        let mut read_page = Page::new(BLOCK_SIZE, BLOCK_SIZE);
        wrt_mgr
            .read_page_from_disk(&mut read_page, &page_no)
            .expect("Failed to read page");

        // Verify that the read data matches the written data
        assert_eq!(page.get_pg_ctr_bytes(), read_page.get_pg_ctr_bytes());

        let mut mirror_page = Page::new(BLOCK_SIZE, BLOCK_SIZE);
        wrt_mgr
            .read_page_from_mirror(&mut mirror_page, &page_no)
            .expect("Failed to mirror page");

        // Verify that the mirror data matches the written data
        assert_eq!(page.get_pg_ctr_bytes(), mirror_page.get_pg_ctr_bytes());

        // Write just to the primary a blank page
        let primary_page = Page::new(BLOCK_SIZE, BLOCK_SIZE);
        wrt_mgr
            .write_page_to_primary(&primary_page, &page_no)
            .expect("Failed to write page");
        // Read again.
        wrt_mgr
            .read_page_from_disk(&mut read_page, &page_no)
            .expect("Failed to read page");
        assert_eq!(
            read_page.get_pg_ctr_bytes(),
            primary_page.get_pg_ctr_bytes()
        );
        wrt_mgr
            .read_page_from_mirror(&mut mirror_page, &page_no)
            .expect("Failed to mirror page");
        assert_ne!(read_page.get_pg_ctr_bytes(), mirror_page.get_pg_ctr_bytes());
    }
}
