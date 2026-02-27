use crate::block_layer::PageConfig;
use crate::version_holder::VersionHolder;
use byteorder::{LittleEndian, ReadBytesExt};
use std::convert::TryFrom;
use std::io::Cursor;

#[derive(PartialEq, Eq, Debug)]
pub enum PageType {
    // A page that can be reused. Created when DB file grows.
    Free = 1,
    // Page created when DB is created and not changed.
    DbRoot = 2,
    // Holds data in the B+ tree, the leaf nodes.
    TreeLeaf = 3,
    // There are two DbMaster pages - one is current and one is old.
    // These are flipped when a new version is committed.
    DbMaster = 4,
    // Page to hold large key/value data. These can be chained if needed.
    Overflow = 5,
    // Page to track free pages.
    FreeDir = 6,
    // B+ tree internal node page.
    TreeDirPage = 7,
}

impl TryFrom<u8> for PageType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(PageType::Free),
            2 => Ok(PageType::DbRoot),
            3 => Ok(PageType::TreeLeaf),
            4 => Ok(PageType::DbMaster),
            5 => Ok(PageType::Overflow),
            6 => Ok(PageType::FreeDir),
            7 => Ok(PageType::TreeDirPage),
            _ => Err(()),
        }
    }
}

pub trait PageTrait {
    fn get_page_bytes(&self) -> &[u8];
    fn get_page_number(&self) -> u64;
    fn set_page_number(&mut self, page_no: u64) -> ();
    fn get_page(&mut self) -> &mut Page;
    fn get_version(&self) -> u64;
    fn set_version(&mut self, version: u64) -> ();
}

// | Page No (u64) | VersionHolder (8 bytes) | Body | )
pub struct Page {
    bytes: Vec<u8>,
    pub page_size: usize,
    pub block_size: usize,
}

impl PageTrait for Page {
    fn get_page_bytes(&self) -> &[u8] {
        &self.bytes[0..self.page_size]
    }

    fn get_page_number(&self) -> u64 {
        let mut cursor = Cursor::new(&self.bytes[..]);
        cursor.set_position(0);
        cursor.read_u64::<LittleEndian>().unwrap()
    }

    fn set_page_number(&mut self, page_no: u64) -> () {
        self.bytes[0..0 + 8].copy_from_slice(&page_no.to_le_bytes());
    }

    fn get_page(&mut self) -> &mut Page {
        self
    }

    fn get_version(&self) -> u64 {
        VersionHolder::from_bytes(self.bytes[8..8 + 8].to_vec()).get_version()
    }

    fn set_version(&mut self, version: u64) -> () {
        let mut version_holder = VersionHolder::from_bytes(self.bytes[8..8 + 8].to_vec());
        version_holder.set_version(version);
        self.bytes[8..8 + 8].copy_from_slice(&version_holder.get_bytes());
    }
}

impl Page {
    pub fn create_new(page_meta: &PageConfig) -> Self {
        Page {
            bytes: vec![0u8; page_meta.block_size],
            block_size: page_meta.block_size,
            page_size: page_meta.page_size,
        }
    }

    pub fn new(block_size: usize, page_size: usize) -> Self {
        Page {
            bytes: vec![0u8; block_size as usize],
            block_size: block_size,
            page_size: page_size,
        }
    }

    pub fn from_bytes(bytes: Vec<u8>, block_size: usize, page_size: usize) -> Self {
        Page {
            bytes,
            block_size: block_size,
            page_size: page_size,
        }
    }

    pub fn get_page_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[0..self.page_size]
    }

    pub fn get_block_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn get_block_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }

    pub fn get_type(&self) -> PageType {
        PageType::try_from(VersionHolder::from_bytes(self.bytes[8..8 + 8].to_vec()).get_flags())
            .unwrap()
    }

    pub fn set_type(&mut self, page_type: PageType) {
        let mut version_holder = VersionHolder::from_bytes(self.bytes[8..8 + 8].to_vec());
        version_holder.set_flags(page_type as u8);
        self.bytes[8..8 + 8].copy_from_slice(&version_holder.get_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let mut page = Page::new(4096, 4092);
        assert_eq!(page.get_page_bytes().len(), 4092);
        assert_eq!(page.get_page_number(), 0);
        page.set_page_number(42);
        page.set_type(PageType::TreeLeaf);
        assert_eq!(page.get_page_number(), 42);
        assert_eq!(page.get_type() as u8, PageType::TreeLeaf as u8);
    }
}
