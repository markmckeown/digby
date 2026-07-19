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
        let offset = page.get_pg_ctr_bytes().len() as u64 - 4;
        let mut cursor = Cursor::new(page.get_pg_ctr_bytes_mut());
        cursor.set_position(offset);
        cursor
            .write_u32::<LittleEndian>(checksum)
            .expect("Failed to write checksum");
    }

    pub fn verify_checksum(page: &Page) {
        let calculated_checksum = xxh32(&page.get_page_bytes()[0..], 0);
        let offset = page.get_pg_ctr_bytes().len() as u64 - 4;
        let mut cursor = std::io::Cursor::new(page.get_pg_ctr_bytes());
        cursor.set_position(offset);
        let stored_checksum = cursor.read_u32::<LittleEndian>().unwrap();
        assert!(
            stored_checksum == calculated_checksum,
            "Calculated checksum does not match stored checksum for page {}",
            page.get_page_number().to_u64()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_no::PageNo;

    #[test]
    #[should_panic(expected = "Calculated checksum does not match stored checksum")]
    fn test_checksum() {
        let mut page = Page::new(4096, 4092);
        page.set_page_number(PageNo::from_u64(42));
        XxHashSanity::set_checksum(&mut page);
        XxHashSanity::verify_checksum(&page);
        // Modify the page and verify that checksum verification fails
        page.set_version(34); // Corrupt the page
        XxHashSanity::verify_checksum(&page); // This should panic due to checksum mismatch
    }
}
