
pub struct TreeDirEntry {
    key: Vec<u8>,
    page_no: u32,
    serialized: Vec<u8>,
}

impl TreeDirEntry {
    pub fn new(key: Vec<u8>, page_no: u32) -> Self {
        assert!(key.len() < u16::MAX as usize);
        let mut serialized = Vec::new(); 
        serialized.extend_from_slice(&(key.len() as u16).to_le_bytes());
        serialized.extend_from_slice(&key);
        serialized.extend_from_slice(&page_no.to_le_bytes());

        TreeDirEntry {
            key,
            page_no,
            serialized
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        use std::io::{Cursor, Read}; 
        use byteorder::{LittleEndian, ReadBytesExt};

        let mut cursor = Cursor::new(&bytes[..]);
        let key_len = cursor.read_u16::<LittleEndian>().unwrap();
        let mut key = vec![0u8; key_len as usize];
        cursor.read_exact(&mut key).unwrap();
        let page_no = cursor.read_u32::<LittleEndian>().unwrap();

        TreeDirEntry { 
            key,
            page_no,
            serialized: bytes
         }
    }

    pub fn get_key(&self) -> &[u8] {
        &self.key
    }

    pub fn get_page_no(&self) -> u32 {
        self.page_no
    }

    pub fn get_serialized(&self) -> &[u8] {
        &self.serialized
    }

    pub fn get_byte_size(&self) -> usize {
        self.serialized.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tree_dir_entry() {
        let tree_dir_entry1 = TreeDirEntry::new(b"mmk".to_vec(), 45);
        assert!(b"mmk".to_vec() == tree_dir_entry1.get_key());
        assert!(45 == tree_dir_entry1.get_page_no());
        let tree_dir_entry2 = TreeDirEntry::from_bytes(tree_dir_entry1.get_serialized().to_vec());
        assert!(b"mmk".to_vec() == tree_dir_entry2.get_key());
        assert!(45 == tree_dir_entry2.get_page_no());
    }
}