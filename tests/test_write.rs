use std::fs::{OpenOptions, File};
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

#[test]
fn test_write_file() {
    let path = "/tmp/test_output.txt";
    let data = b"Hello, Digby!";
    
    let mut f = File::create(path).expect("Should be able to create file");
    f.write_all(data).expect("Should be able to write data");
    f.sync_all().expect("Should be able to sync data");
    
    let content = std::fs::read_to_string(path).expect("Should be able to read file");
    assert_eq!(content, "Hello, Digby!");
    // Clean up
    std::fs::remove_file(path).expect("Failed to remove file");
}


#[test]
fn test_write_db_page() {
    let path = "/tmp/test_db_page.db";
    let page_number = 2;
    let page_size: u64 = 4096;
    let page_data = vec![0u8; page_size as usize]; // A page filled with zeros

    write_page_to_disk(path, page_number, &page_data, page_size).expect("Should be able to write DB page");

    let mut file = File::open(path).expect("Should be able to open file");
    let mut buffer = vec![0u8; page_size as usize];
    file.seek(SeekFrom::Start(page_number * page_size)).expect("Should be able to seek");
    file.read_exact(&mut buffer).expect("Should be able to read data");

    assert_eq!(buffer, page_data);
    // Clean up
    std::fs::remove_file(path).expect("Failed to remove file");
}

fn write_page_to_disk(path: &str, page_number: u64, page_data: &[u8], page_size: u64) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)?;

    let offset = page_number * page_size;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(page_data)?;
    file.sync_all()?;
    Ok(())
}