use crate::{Aes128GcmSanity, Page, XxHash3Sanity, XxHashSanity};

// Used to check a block read from disk is not
// corrupt. This is done either by recording
// a checksum of the page within the block,
// or encrypting the page in the block.
// Three approaches are supported at present, xxhash 32
// or xxhash3 64 as a checksum or AES-128-GCM
// encryption of the block.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum BlockSanity {
    XxH32Checksum = 0,
    Aes128Gcm = 1,
    XxH64Checksum = 2,
}

impl TryFrom<u8> for BlockSanity {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(BlockSanity::XxH32Checksum),
            1 => Ok(BlockSanity::Aes128Gcm),
            2 => Ok(BlockSanity::XxH64Checksum),
            _ => Err(()),
        }
    }
}

impl From<BlockSanity> for u8 {
    fn from(value: BlockSanity) -> Self {
        match value {
            BlockSanity::XxH32Checksum => 0,
            BlockSanity::Aes128Gcm => 1,
            BlockSanity::XxH64Checksum => 2,
        }
    }
}

impl BlockSanity {
    pub const fn get_bytes_used(block_sanity_type: BlockSanity) -> usize {
        match block_sanity_type {
            BlockSanity::XxH32Checksum => 4,
            BlockSanity::XxH64Checksum => 8,
            BlockSanity::Aes128Gcm => 28,
        }
    }

    pub fn check_block_sanity(&self, page: &mut Page, key: &Vec<u8>) {
        match self {
            BlockSanity::XxH32Checksum => {
                XxHashSanity::verify_checksum(page);
            }
            BlockSanity::XxH64Checksum => {
                XxHash3Sanity::verify_checksum(page);
            }
            BlockSanity::Aes128Gcm => {
                Aes128GcmSanity::decrypt_page(page, key);
            }
        }
    }

    pub fn set_block_sanity(&self, page: &mut Page, key: &Vec<u8>) {
        match self {
            BlockSanity::XxH32Checksum => {
                XxHashSanity::set_checksum(page);
            }
            BlockSanity::XxH64Checksum => {
                XxHash3Sanity::set_checksum(page);
            }
            BlockSanity::Aes128Gcm => {
                Aes128GcmSanity::encrypt_page(page, key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_sanity_bytes_used() {
        assert_eq!(BlockSanity::get_bytes_used(BlockSanity::XxH32Checksum), 4);
        assert_eq!(BlockSanity::get_bytes_used(BlockSanity::XxH64Checksum), 8);
        assert_eq!(BlockSanity::get_bytes_used(BlockSanity::Aes128Gcm), 28);
    }

    #[test]
    fn test_block_sanity_try_from() {
        assert_eq!(
            BlockSanity::try_from(0).unwrap(),
            BlockSanity::XxH32Checksum
        );
        assert_eq!(
            BlockSanity::try_from(2).unwrap(),
            BlockSanity::XxH64Checksum
        );
        assert_eq!(BlockSanity::try_from(1).unwrap(), BlockSanity::Aes128Gcm);
        assert!(BlockSanity::try_from(3).is_err());
    }
}
