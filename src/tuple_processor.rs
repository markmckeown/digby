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
        key: &[u8],
        value: &[u8],
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

        let overflow_tuple: OverflowTuple = if compressor.compressor_type != CompressorType::None {
            let overflow_key = compressor.compress(key);
            let overflow_value = compressed_value;
            OverflowTuple::new(
                &overflow_key,
                &overflow_value,
                version,
                Overflow::KeyValueCompressed,
            )
        } else {
            OverflowTuple::new(key, value, version, Overflow::None)
        };

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

        Tuple::new_with_overflow(
            key,
            overflow_page_no.to_le_bytes().to_vec().as_ref(),
            version,
            overflow_type,
        )
    }

    pub fn is_oversized_key(key: &[u8]) -> bool {
        if key.len() > u8::MAX as usize {
            return true;
        }
        false
    }

    pub fn generate_short_key(key: &[u8]) -> Vec<u8> {
        assert!(key.len() > u8::MAX as usize);
        let key_hash = Sha256::digest(key);
        let mut new_key: Vec<u8> = Vec::with_capacity(u8::MAX as usize);
        new_key.extend_from_slice(&key[0..u8::MAX as usize - 32]);
        new_key.extend_from_slice(&key_hash);
        assert!(new_key.len() == u8::MAX as usize);
        new_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compressor::{Compressor, CompressorType};
    use crate::block_layer::BlockLayer;
    use crate::file_layer::FileLayer;
    use tempfile::NamedTempFile;
    use crate::tuple::TupleTrait;
    use crate::page::PageTrait;

    #[test]
    fn test_tuple_processor_oversized_key() {
        let small_key = vec![0u8; 255];
        assert!(!TupleProcessor::is_oversized_key(&small_key));

        let large_key = vec![0u8; 256];
        assert!(TupleProcessor::is_oversized_key(&large_key));
    }

    #[test]
    fn test_generate_short_key() {
        let mut large_key = vec![0u8; 256];
        large_key[0] = 1;
        let short_key = TupleProcessor::generate_short_key(&large_key);
        assert_eq!(short_key.len(), 255);
        assert_eq!(short_key[0], 1);
    }

    #[test]
    fn test_generate_tuple() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(temp_file.path())
            .unwrap();
        let file_layer = FileLayer::new(file, 4096);
        let block_layer = BlockLayer::new(file_layer, 4096);
        let mut page_cache = PageCache::new(block_layer);
        let version = 0;
        let new_version = 1;

        let free_dir_page_no = *page_cache.generate_free_pages(1).get(0).unwrap();
        let mut free_dir_page =
            crate::FreeDirPage::create_new(page_cache.get_page_config(), free_dir_page_no, version);
        page_cache.put_page(free_dir_page.get_page());
        let mut free_page_tracker = FreePageTracker::new(
            page_cache.get_page(free_dir_page_no),
            new_version,
            *page_cache.get_page_config(),
        );

        let compressor_none = Compressor::new(CompressorType::None);

        let small_key = vec![1u8; 10];
        let small_value = vec![2u8; 10];
        let tuple = TupleProcessor::generate_tuple(
            &small_key,
            &small_value,
            &mut page_cache,
            &mut free_page_tracker,
            1,
            &compressor_none,
        );
        assert_eq!(tuple.get_overflow(), Overflow::None);

        let compressor_lz4 = Compressor::new(CompressorType::LZ4);
        let compressible_value = vec![2u8; 2000];
        let tuple_compressed = TupleProcessor::generate_tuple(
            &small_key,
            &compressible_value,
            &mut page_cache,
            &mut free_page_tracker,
            1,
            &compressor_lz4,
        );
        assert_eq!(tuple_compressed.get_overflow(), Overflow::ValueCompressed);

        let large_value = vec![3u8; 2000];
        let tuple_large_val = TupleProcessor::generate_tuple(
            &small_key,
            &large_value,
            &mut page_cache,
            &mut free_page_tracker,
            1,
            &compressor_none,
        );
        assert_eq!(tuple_large_val.get_overflow(), Overflow::ValueOverflow);

        let large_key = vec![4u8; 300];
        let tuple_large_key = TupleProcessor::generate_tuple(
            &large_key,
            &small_value,
            &mut page_cache,
            &mut free_page_tracker,
            1,
            &compressor_none,
        );
        assert_eq!(tuple_large_key.get_overflow(), Overflow::KeyOverflow);

        let tuple_large_both = TupleProcessor::generate_tuple(
            &large_key,
            &large_value,
            &mut page_cache,
            &mut free_page_tracker,
            1,
            &compressor_none,
        );
        assert_eq!(tuple_large_both.get_overflow(), Overflow::KeyValueOverflow);

        let tuple_large_both_comp = TupleProcessor::generate_tuple(
            &large_key,
            &large_value,
            &mut page_cache,
            &mut free_page_tracker,
            1,
            &compressor_lz4,
        );
        assert_eq!(tuple_large_both_comp.get_overflow(), Overflow::KeyValueOverflow);
    }
}
