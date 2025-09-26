use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Cursor, Read}; 

pub struct TableDirEntry {
    root_page_number: u32,
    version: u64,
    name : Vec<u8>,
    serialized: Vec<u8>
}


impl TableDirEntry {
    pub fn new(name: Vec<u8>, root_page_number: u32, version: u64) -> Self {
        let mut serialized = Vec::new();
        serialized.extend_from_slice(&root_page_number.to_le_bytes());
        serialized.extend_from_slice(&version.to_le_bytes());
        serialized.extend_from_slice(&(name.len() as u8).to_le_bytes());
        serialized.extend_from_slice(&name);

        TableDirEntry {
            root_page_number,
            version,
            name,
            serialized,
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let mut cursor = Cursor::new(&bytes[..]);

        let root_page_number = cursor.read_u32::<LittleEndian>().unwrap();
        let version = cursor.read_u64::<LittleEndian>().unwrap();
        let name_length = cursor.read_u8().unwrap();
        let mut name = vec![0u8; name_length as usize];
        cursor.read_exact(&mut name).unwrap();

        TableDirEntry {
            root_page_number,
            version,
            name,
            serialized: bytes
        }
    }

    pub fn get_version(&self) -> u64 {
        self.version
    }

    pub fn get_root_page_number(&self) -> u32 {
        self.root_page_number
    }

    pub fn get_byte_size(&self) -> usize {
        self.serialized.len()
    }

    pub fn get_name(&self) -> &[u8] {
        &self.name
    }

    pub fn get_serialized(&self) -> &[u8] {
        &self.serialized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_basic() {
        let entry = TableDirEntry::new(b"mmk".to_vec(), 67, 567_);
        // mmk == 3
        // name_size == 1
        // page_number == 4
        // version == 8
        assert!(16 == entry.get_byte_size());
        assert!(67 == entry.get_root_page_number());
        assert!(567 == entry.get_version());
        assert!(b"mmk".to_vec() == entry.get_name());
    }

    #[test]
    fn test_entry_serialise() {
        let first_entry = TableDirEntry::new(b"mmk".to_vec(), 67, 567_); 
        let entry = TableDirEntry::from_bytes(first_entry.get_serialized().to_vec());
        // mmk == 3
        // name_size == 1
        // page_number == 4
        // version == 8
        assert!(16 == entry.get_byte_size());
        assert!(67 == entry.get_root_page_number());
        assert!(567 == entry.get_version());
        assert!(b"mmk".to_vec() == entry.get_name());
    }

}
