use crate::Tuple; 
use crate::tuple::TupleTrait;

pub struct TableDirEntry {
    entry: Tuple,
}

// Just wrap a tuple.
impl TableDirEntry {
    pub fn new(name: Vec<u8>, root_page_number: u32, version: u64) -> Self {
        assert!(name.len() < 255, "Table name too long");
        let entry = Tuple::new(name.as_ref(), root_page_number.to_le_bytes().to_vec().as_ref(), version);

        TableDirEntry {
            entry
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let entry = Tuple::from_bytes(bytes);
        
        TableDirEntry {
            entry
        }
    }

    pub fn get_version(&self) -> u64 {
        self.entry.get_version()
    }

    pub fn get_root_page_number(&self) -> u32 {
        u32::from_le_bytes(self.entry.get_value().try_into().unwrap())
    }

    pub fn get_byte_size(&self) -> usize {
        self.entry.get_byte_size()
    }

    pub fn get_name(&self) -> &[u8] {
        &self.entry.get_key()
    }

    pub fn get_serialized(&self) -> &[u8] {
        &self.entry.get_serialized()
    }

    pub fn get_tuple(&self) -> &Tuple {
        &self.entry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_basic() {
        let entry = TableDirEntry::new(b"mmk".to_vec(), 67, 567_);
        let size =  entry.get_byte_size();
        assert!(19 == size);
        assert!(67 == entry.get_root_page_number());
        assert!(567 == entry.get_version());
        assert!(b"mmk".to_vec() == entry.get_name());
    }

    #[test]
    fn test_entry_serialise() {
        let first_entry = TableDirEntry::new(b"mmk".to_vec(), 67, 567_); 
        let entry = TableDirEntry::from_bytes(first_entry.get_serialized().to_vec());
        assert!(19 == entry.get_byte_size());
        assert!(67 == entry.get_root_page_number());
        assert!(567 == entry.get_version());
        assert!(b"mmk".to_vec() == entry.get_name());
    }

}
