use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use std::convert::TryFrom;

use crate::page;



#[derive(PartialEq, Eq)]
pub enum PageType {
    Header = 1,
    Data = 2,
}

impl TryFrom<u8> for PageType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(PageType::Header),
            2 => Ok(PageType::Data),
            _ => Err(()),
        }
    }
}

trait PageTrait {
    fn get_bytes(&self) -> &[u8];
    fn get_page_number(&mut self) -> u32;
}


pub struct HeadPage {
    page: Page
}

impl PageTrait for HeadPage {
    fn get_bytes(&self) -> &[u8] {
        self.page.get_bytes()
    }

    fn get_page_number(&mut self) -> u32 {
        self.page.get_page_number()
    }
}

impl HeadPage {
    pub fn new(page_size: u64) -> Self {
        let mut head_page = HeadPage {
            page: Page::new(page_size),
        };
        head_page.page.set_type(PageType::Header);
        head_page.page.set_page_number(0);
        head_page
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let mut page = Page::from_bytes(bytes);
        if page.get_type() != page::PageType::Header {
            panic!("Invalid page type for HeadPage");
        }
        if page.get_page_number() != 0 {
            panic!("Invalid page number for HeadPage");
        }
        HeadPage { page }
    }

    pub fn from_page(mut page: Page) -> Self {
        if page.get_type() != page::PageType::Header {
            panic!("Invalid page type for HeadPage");
        }
        if page.get_page_number() != 0 {
            panic!("Invalid page number for HeadPage");
        }
        HeadPage { page }
    }
}




// Checksum(u32) | Page No (u32)| Type(u8) | Reserved(3 bytes) | Data(4084 bytes)
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

    pub fn get_checksum(&mut self) -> u32 {
        let mut cursor = Cursor::new(&mut self.bytes[..]);
        cursor.set_position(0);
        cursor.read_u32::<LittleEndian>().unwrap()
    }

    pub fn set_checksum(&mut self, checksum: u32) {
        let mut cursor = Cursor::new(&mut self.bytes[..]);
        cursor.set_position(0);
        cursor.write_u32::<LittleEndian>(checksum as u32).expect("Failed to write checksum");
    }

     pub fn get_type(&mut self) -> PageType {
        let mut cursor = Cursor::new(&mut self.bytes[..]);
        cursor.set_position(8);
        PageType::try_from(cursor.read_u8().unwrap()).expect("Invalid page type")
     }

    pub fn set_type(&mut self, page_type: PageType) {
        let mut cursor = Cursor::new(&mut self.bytes[..]);
        cursor.set_position(8);
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
        page.set_checksum(23);
        page.set_page_number(42);
        page.set_type(PageType::Data);
        assert_eq!(page.get_page_number(), 42);
        assert_eq!(page.get_checksum(), 23);
        assert_eq!(page.get_type() as u8, PageType::Data as u8);
    }

}
