use crate::compressor::Compressor;
use crate::compressor::CompressorType;
use crate::{
    FreePageTracker, OverflowPageHandler, OverflowTuple, PageCache,
    tuple::{Overflow, Tuple},
};
use sha2::{Digest, Sha256};

pub struct TupleProcessor {}

// If key is over 256 bytes then it is stored as [ first 224 bytes of key | SHA256 of Key].
// The tuple will be stored in an Overflow page with the full key.
// If keys are larger 256 bytes then lexical sorting will break down - another option
// would be just to store the SHA256 as the comppressed key.
impl TupleProcessor {
    const MAX_VALUE_SIZE: usize = 1024;

    pub fn generate_tuple(
        key: &Vec<u8>,
        value: &Vec<u8>,
        page_cache: &mut PageCache,
        free_page_tracker: &mut FreePageTracker,
        version: u64,
        compressor: &Compressor,
    ) -> Tuple {
        if !TupleProcessor::is_oversized_key(key) && value.len() < TupleProcessor::MAX_VALUE_SIZE {
            return Tuple::new(key, value, version);
        }
        assert!(key.len() < u32::MAX as usize, "key is too large");
        assert!(value.len() < u32::MAX as usize, "value is too large");

        let mut compressed_value: Vec<u8> = Vec::new();
        if compressor.compressor_type != CompressorType::None {
            compressed_value = compressor.compress(value);
            // We can store it with the value compressed.
            if !TupleProcessor::is_oversized_key(key)
                && compressed_value.len() < TupleProcessor::MAX_VALUE_SIZE
            {
                return Tuple::new_with_overflow(
                    key,
                    &compressed_value,
                    version,
                    Overflow::ValueCompressed,
                );
            }
        }

        let overflow_type: Overflow;
        if TupleProcessor::is_oversized_key(key) && value.len() > TupleProcessor::MAX_VALUE_SIZE {
            overflow_type = Overflow::KeyValueOverflow;
        } else if key.len() > u8::MAX as usize {
            overflow_type = Overflow::KeyOverflow;
        } else {
            overflow_type = Overflow::ValueOverflow
        }

        let overflow_tuple: OverflowTuple;
        if compressor.compressor_type != CompressorType::None {
            let overflow_key = compressor.compress(key);
            let overflow_value = compressed_value;
            overflow_tuple = OverflowTuple::new(
                &overflow_key,
                &overflow_value,
                version,
                Overflow::KeyValueCompressed,
            );
        } else {
            overflow_tuple = OverflowTuple::new(&key, &value, version, Overflow::None);
        }

        let overflow_page_no = OverflowPageHandler::store_overflow_tuple(
            overflow_tuple,
            page_cache,
            free_page_tracker,
            version,
        );

        if TupleProcessor::is_oversized_key(key) {
            let new_key = TupleProcessor::generate_short_key(key);
            return Tuple::new_with_overflow(
                &new_key,
                overflow_page_no.to_le_bytes().to_vec().as_ref(),
                version,
                overflow_type,
            );
        }

        return Tuple::new_with_overflow(
            key,
            overflow_page_no.to_le_bytes().to_vec().as_ref(),
            version,
            overflow_type,
        );
    }

    pub fn is_oversized_key(key: &Vec<u8>) -> bool {
        if key.len() > u8::MAX as usize {
            return true;
        }
        return false;
    }

    pub fn generate_short_key(key: &Vec<u8>) -> Vec<u8> {
        assert!(key.len() > u8::MAX as usize);
        let key_hash = Sha256::digest(key);
        let mut new_key: Vec<u8> = Vec::with_capacity(u8::MAX as usize);
        new_key.extend_from_slice(&key[0..u8::MAX as usize - 32]);
        new_key.extend_from_slice(&key_hash);
        assert!(new_key.len() == u8::MAX as usize);
        return new_key;
    }
}
