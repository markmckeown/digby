use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use std::convert::TryFrom;


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
            _ => Err(()),
        }
    }
}

pub trait PageTrait {
    fn get_bytes(&self) -> &[u8];
    fn get_page_number(&mut self) -> u32;
    fn get_page(&mut self) -> &mut Page;
    fn get_version(&mut self) -> u64;
    fn set_version(&mut self, version: u64) -> ();
}


// | Checksum(u32) | Page No (u32) | Version (u64) | Type(u8) | Reserved(3 bytes) | Data(4084 bytes)
pub struct Page {
    bytes: Vec<u8>
}

impl PageTrait for Page {
    fn get_bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn get_page_number(&mut self) -> u32 {
        let mut cursor = Cursor::new(&mut self.bytes[..]);
        cursor.set_position(4);
        cursor.read_u32::<LittleEndian>().unwrap()
    }

    fn get_page(&mut self) -> &mut Page {
        self
    }

    fn get_version(&mut self) -> u64 {
        let mut cursor = Cursor::new(&mut self.bytes[..]);
        cursor.set_position(8);
        cursor.read_u64::<LittleEndian>().unwrap()
    }

    fn set_version(&mut self, version: u64) -> () {
        let mut cursor = Cursor::new(&mut self.bytes[..]);
        cursor.set_position(8);
        cursor.write_u64::<LittleEndian>(version as u64).expect("Failed to write version");     
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


    pub fn get_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }

    
    pub fn set_page_number(&mut self, page_number: u32) {
        let mut cursor = Cursor::new(&mut self.bytes[..]);
        cursor.set_position(4);
        cursor.write_u32::<LittleEndian>(page_number as u32).expect("Failed to write page number");
    }

    pub fn get_type(&mut self) -> PageType {
        let mut cursor = Cursor::new(&mut self.bytes[..]);
        cursor.set_position(16);
        PageType::try_from(cursor.read_u8().unwrap()).expect("Invalid page type")
     }

    pub fn set_type(&mut self, page_type: PageType) {
        let mut cursor = Cursor::new(&mut self.bytes[..]);
        cursor.set_position(16);
        cursor.write_i8(page_type as i8).expect("Failed to write page type");
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
        let mut page = Page::new(4096);
        assert_eq!(page.get_bytes().len(), 4096);
        assert_eq!(page.get_page_number(), 0);
        page.set_page_number(42);
        page.set_type(PageType::TreeLeaf);
        assert_eq!(page.get_page_number(), 42);
        assert_eq!(page.get_type() as u8, PageType::TreeLeaf as u8);
    }

}
