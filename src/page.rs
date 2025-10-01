use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor};
use std::convert::TryFrom;
use crate::version_holder::VersionHolder;


#[derive(PartialEq, Eq)]
pub enum PageType {
    Free = 1,
    DbRoot = 2,
    TreeLeaf = 3,
    DbMaster = 4,
    Overflow = 5,
    FreeDir = 6,
    TreeInternal = 7,
    TreeRoot = 8,
    TableDir = 9,
    TreeRootSingle = 10,
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
            7 => Ok(PageType::TreeInternal),
            8 => Ok(PageType::TreeRoot),
            9 => Ok(PageType::TableDir),
            10 => Ok(PageType::TreeRootSingle),
            _ => Err(()),
        }
    }
}

pub trait PageTrait {
    fn get_bytes(&self) -> &[u8];
    fn get_page_number(& self) -> u32;
    fn set_page_number(&mut self, page_no: u32) -> (); 
    fn get_page(&mut self) -> &mut Page;
    fn get_version(& self) -> u64;
    fn set_version(&mut self, version: u64) -> ();
}


// | Checksum(u32) | Page No (u32) | VersionHolder (8 bytes) | Data(4084 bytes)
pub struct Page {
    bytes: Vec<u8>
}

impl PageTrait for Page {
    fn get_bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn get_page_number(&self) -> u32 {
        let mut cursor = Cursor::new(&self.bytes[..]);
        cursor.set_position(4);
        cursor.read_u32::<LittleEndian>().unwrap()
    }

    fn set_page_number(&mut self, page_no: u32) -> () {
        self.bytes[4..4+4].copy_from_slice(&page_no.to_le_bytes());
    }


    fn get_page(&mut self) -> &mut Page {
        self
    }

    fn get_version(& self) -> u64 {
        VersionHolder::from_bytes(self.bytes[8..8+8].to_vec()).get_version()
    }

    fn set_version(&mut self, version: u64) -> () {
        let mut version_holder = VersionHolder::from_bytes(self.bytes[8..8+8].to_vec());
        version_holder.set_version(version);
        self.bytes[8..8+8].copy_from_slice(&version_holder.get_bytes());
    }
}


impl Page {
    pub fn new(page_size: u64) -> Self {
        Page {
            bytes: vec![0u8; page_size as usize],
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Page {
            bytes,
        }
    }


    pub fn copy_page_body(&mut self, from: impl PageTrait, page_size: u64) -> () {
        self.bytes[8..page_size as usize].copy_from_slice(&from.get_bytes()[8..4096]);
    }

    pub fn get_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
    
    pub fn set_page_number(&mut self, page_number: u32) {
        let mut cursor = Cursor::new(&mut self.bytes[..]);
        cursor.set_position(4);
        cursor.write_u32::<LittleEndian>(page_number as u32).expect("Failed to write page number");
    }

    pub fn get_type(&self) -> PageType {
        PageType::try_from(VersionHolder::from_bytes(self.bytes[8..8+8].to_vec()).get_flags()).unwrap()
     }

    pub fn set_type(&mut self, page_type: PageType) {
        let mut version_holder = VersionHolder::from_bytes(self.bytes[8..8+8].to_vec());
        version_holder.set_flags(page_type as u8);
        self.bytes[8..8+8].copy_from_slice(&version_holder.get_bytes());
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let mut page = Page::new(4096);
        assert_eq!(page.get_bytes().len(), 4096);
        assert_eq!(page.get_page_number(), 0);
        page.set_page_number(42);
        page.set_type(PageType::TreeLeaf);
        assert_eq!(page.get_page_number(), 42);
        assert_eq!(page.get_type() as u8, PageType::TreeLeaf as u8);
    }

}
