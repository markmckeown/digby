use byteorder::ReadBytesExt;
use byteorder::{LittleEndian, WriteBytesExt};
use digby::Db;
use digby::compressor::CompressorType;
use std::fs::{File, OpenOptions};
use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

#[test]
fn test_digby_db() {
    let mut _db = Db::new("/tmp/test_db.db", None, CompressorType::None);
    let _ = std::fs::remove_file("/tmp/test_db.db");
}

#[test]
fn test_write_db_page() {
    let path = "/tmp/test_db_page.db";
    let page_number = 2;
    let page_size: u64 = 4096;
    let mut page_data = vec![0u8; page_size as usize]; // A page filled with zeros

    let magic_number: u32 = 26061973;

    let mut cursor = Cursor::new(&mut page_data[..]);
    cursor
        .write_u32::<LittleEndian>(magic_number)
        .expect("Should be able to write magic number");

    write_page_to_disk(path, page_number, &page_data, page_size)
        .expect("Should be able to write DB page");

    let mut file = File::open(path).expect("Should be able to open file");
    let mut buffer = vec![0u8; page_size as usize];
    file.seek(SeekFrom::Start(page_number * page_size))
        .expect("Should be able to seek");
    file.read_exact(&mut buffer)
        .expect("Should be able to read data");

    let mut read_cursor = Cursor::new(&buffer[..]);
    let read_magic = read_cursor
        .read_u32::<LittleEndian>()
        .expect("Should be able to read magic number");
    assert_eq!(read_magic, magic_number);

    assert_eq!(buffer, page_data);
    // Clean up
    std::fs::remove_file(path).expect("Failed to remove file");
}

fn write_page_to_disk(
    path: &str,
    page_number: u64,
    page_data: &[u8],
    page_size: u64,
) -> std::io::Result<()> {
    let mut file = OpenOptions::new().write(true).create(true).open(path)?;

    let offset = page_number * page_size;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(page_data)?;
    file.sync_all()?;
    Ok(())
}
