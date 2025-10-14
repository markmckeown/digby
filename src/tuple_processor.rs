use crate::{tuple::{Overflow, Tuple}, FreePageTracker, OverflowPageHandler, OverflowTuple, PageCache};
use sha2::{Digest, Sha256};

pub struct TupleProcessor {

}

impl TupleProcessor {
    pub fn generate_tuple(key: &Vec<u8>, 
        value: &Vec<u8>, 
        page_cache: &mut PageCache, 
        free_page_tracker: &mut FreePageTracker,
        version: u64,
        page_size: usize) -> Tuple {
        if key.len() < u8::MAX as usize && value.len() < 2048 {
            return Tuple::new(key, value, version);
        }    
        assert!(key.len() < u32::MAX as usize, "key is too large");
        assert!(value.len() < u32::MAX as usize, "value is too large");

        let overflow_type: Overflow;
        if key.len() > u8::MAX as usize && value.len() > 2048 {
            overflow_type = Overflow::KeyValueOverflow;
        } else if key.len() > u8::MAX as usize {
            overflow_type = Overflow::KeyOverflow;
        } else {
            overflow_type = Overflow::ValueOverflow
        }

        let overflow_tuple = OverflowTuple::new(key, value, version, overflow_type);
        let overflow_page_no = OverflowPageHandler::store_overflow_tuple(overflow_tuple, page_cache, 
            free_page_tracker, version, page_size);

        if key.len() > u8::MAX as usize {
            // Generate a short key - first (256 - 32) bytes plus the SHA256 of the key.
            let key_hash = Sha256::digest(key);
            let mut new_key = key[0 .. u8::MAX as usize - 32].to_vec();
            new_key.append(&mut key_hash.to_vec());
            assert!(new_key.len() == u8::MAX as usize);
            return Tuple::new_with_overflow(&new_key, overflow_page_no.to_le_bytes().to_vec().as_ref(), version, overflow_type);
        } 
            
        return Tuple::new_with_overflow(key, overflow_page_no.to_le_bytes().to_vec().as_ref(), version, overflow_type);
    }

    pub fn is_oversized_key(key: &Vec<u8>) -> bool {
        if key.len() > u8::MAX as usize {
            return true;
        }
        return false;
    }

    pub fn generate_short_key(key: &Vec<u8>) -> Vec<u8> {
        let key_hash = Sha256::digest(key);
        let mut new_key = key[0 .. u8::MAX as usize - 32].to_vec();
        new_key.append(&mut key_hash.to_vec());
        assert!(new_key.len() == u8::MAX as usize);
        return new_key;
    }
}