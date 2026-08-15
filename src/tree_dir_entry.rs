use crate::PageNo;

pub struct TreeDirEntry {
    key: Vec<u8>,
    page_no: PageNo,
    serialized: Vec<u8>,
}

impl TreeDirEntry {
    pub fn new(key: Vec<u8>, page_no: PageNo) -> Self {
        assert!(key.len() <= u8::MAX as usize);
        let mut serialized = Vec::new();
        serialized.extend_from_slice(&page_no.get_bytes());
        serialized.push(key.len() as u8);
        serialized.extend_from_slice(&key);

        TreeDirEntry {
            key,
            page_no,
            serialized,
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        use byteorder::{LittleEndian, ReadBytesExt};
        use std::io::{Cursor, Read};

        let mut cursor = Cursor::new(&bytes[..]);
        let page_no = PageNo::from_u64(cursor.read_u64::<LittleEndian>().unwrap());
        let key_len = cursor.read_u8().unwrap();
        let mut key = vec![0u8; key_len as usize];
        cursor.read_exact(&mut key).unwrap();

        TreeDirEntry {
            key,
            page_no,
            serialized: bytes,
        }
    }

    pub fn get_key(&self) -> &[u8] {
        &self.key
    }

    pub fn get_page_no(&self) -> PageNo {
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
    use crate::page::PageType;

    #[test]
    fn test_tree_dir_entry() {
        let tree_dir_entry1 =
            TreeDirEntry::new(b"mmk".to_vec(), PageNo::new(PageType::LeafPage, 0, 45));
        assert_eq!(b"mmk".to_vec(), tree_dir_entry1.get_key());
        assert_eq!(
            PageNo::new(PageType::LeafPage, 0, 45),
            tree_dir_entry1.get_page_no()
        );
        let tree_dir_entry2 = TreeDirEntry::from_bytes(tree_dir_entry1.get_serialized().to_vec());
        assert_eq!(b"mmk".to_vec(), tree_dir_entry2.get_key());
        assert_eq!(
            PageNo::new(PageType::LeafPage, 0, 45),
            tree_dir_entry2.get_page_no()
        );
        assert_eq!(tree_dir_entry1.get_byte_size(), 12);
    }
}
