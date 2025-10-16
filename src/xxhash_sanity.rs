use xxhash_rust::xxh32::xxh32;
use std::io::Cursor;
use crate::page::Page;
use crate::page::PageTrait;
use byteorder::LittleEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};

pub struct XxHashSanity {

}

impl XxHashSanity {
    pub fn set_checksum(page: &mut Page) {
        let checksum = xxh32(&page.get_bytes()[4..], 0);
        let mut cursor = Cursor::new(page.get_bytes_mut());
        cursor.set_position(0);
        cursor.write_u32::<LittleEndian>(checksum as u32).expect("Failed to write checksum");
    }   

    pub fn verify_checksum(page: &mut Page) -> () {
        let mut cursor = std::io::Cursor::new(page.get_bytes());
        cursor.set_position(0);
        let stored_checksum = cursor.read_u32::<LittleEndian>().unwrap();
        let calculated_checksum = xxh32(&page.get_bytes()[4..], 0);
        assert!(stored_checksum == calculated_checksum, 
            "Calculated checksum does not match stored checksum for page {}", page.get_page_number());
    }
}