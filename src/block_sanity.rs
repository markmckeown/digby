use crate::{Aes128GcmSanity, Page, XxHashSanity};

#[derive(PartialEq, Eq, Debug)]
pub enum BlockSanity {
    XxH32Checksum = 0,
    Aes128Gcm = 1,
}

impl TryFrom<u8> for BlockSanity {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(BlockSanity::XxH32Checksum),
            1 => Ok(BlockSanity::Aes128Gcm),
            _ => Err(()),
        }
    }
}

impl From<BlockSanity> for u8 {
    fn from(value: BlockSanity) -> Self {
        match value {
            BlockSanity::XxH32Checksum => 0,
            BlockSanity::Aes128Gcm => 1,
        }
    }
}

impl BlockSanity {
    pub fn get_bytes_used(block_sanity_type: BlockSanity) -> usize {
        match block_sanity_type {
            BlockSanity::XxH32Checksum => 4,
            BlockSanity::Aes128Gcm => 28,
        }
    }

    pub fn check_block_sanity(&self, page: &mut Page, key: &Vec<u8>) -> () {
        match self {
            BlockSanity::XxH32Checksum => {
                XxHashSanity::verify_checksum(page);
            },
            BlockSanity::Aes128Gcm => {
                Aes128GcmSanity::decrypt_page(page, key);
            },
        }
    }


    pub fn set_block_sanity(&self, page: &mut Page, key: &Vec<u8>) -> () {
        match self {
            BlockSanity::XxH32Checksum => {
                XxHashSanity::set_checksum(page);
            },
            BlockSanity::Aes128Gcm => {
                Aes128GcmSanity::encrypt_page(page, key);
            },
        }
    }
}