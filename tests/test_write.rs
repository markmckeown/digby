use std::fs::File;
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