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
