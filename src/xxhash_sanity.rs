use crate::page::Page;
use crate::page::PageTrait;
use byteorder::LittleEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use xxhash_rust::xxh32::xxh32;

pub struct XxHashSanity {}

impl XxHashSanity {
    pub fn set_checksum(page: &mut Page) {
        let checksum = xxh32(&page.get_page_bytes()[0..], 0);
        let offset = page.block_size as u64 - 4;
        let mut cursor = Cursor::new(page.get_block_bytes_mut());
        cursor.set_position(offset);
        cursor
            .write_u32::<LittleEndian>(checksum)
            .expect("Failed to write checksum");
    }

    pub fn verify_checksum(page: &mut Page) {
        let calculated_checksum = xxh32(&page.get_page_bytes()[0..], 0);
        let offset = page.block_size as u64 - 4;
        let mut cursor = std::io::Cursor::new(page.get_block_bytes());
        cursor.set_position(offset);
        let stored_checksum = cursor.read_u32::<LittleEndian>().unwrap();
        assert!(
            stored_checksum == calculated_checksum,
            "Calculated checksum does not match stored checksum for page {}",
            page.get_page_number()
        );
    }
}
