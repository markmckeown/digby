use crate::block_sanity::BlockSanity;
use crate::compressor::CompressorType;
use crate::overflow_page::OverflowPage;


#[derive(Copy, Clone, Debug)]
pub struct DbConfig {
    pub block_size: usize,
    pub page_size: usize,
    pub block_sanity_size: usize,
    pub compressor_type: CompressorType,
    pub block_sanity: BlockSanity,
    pub leaf_page_blk_exp: u8,
    pub dir_page_blk_exp: u8,
    pub overflow_pg_free_space: [usize; 9],
}

impl DbConfig {
    pub const fn builder() -> DbConfigBuilder {
        DbConfigBuilder::new()
    }

    pub const fn get_leaf_page_blk_cnt(&self) -> u64 {
        1 << self.leaf_page_blk_exp
    }

    pub const fn get_dir_page_blk_cnt(&self) -> u64 {
        1 << self.dir_page_blk_exp
    }

    pub const fn get_max_overflow_pg_free_space(&self) -> usize {
        self.overflow_pg_free_space[8]
    }

    pub const fn get_blk_exp_for_size(&self, size: usize) -> u8 {
        let mut i = 0usize;
        while i < self.overflow_pg_free_space.len() {
            if size <= self.overflow_pg_free_space[i] {
                return i as u8;
            }
            i += 1;
        }
        8
    }

    pub const fn get_max_overflow_exp_size(&self) -> u8 {
        8
    }


}

#[derive(Clone, Debug)]
pub struct DbConfigBuilder {
    block_size: usize,
    block_sanity_size: usize,
    compressor_type: CompressorType,
    block_sanity: BlockSanity,
    leaf_page_blk_exp: u8,
    dir_page_blk_exp: u8,
    overflow_pg_free_space: [usize; 9],
}

impl Default for DbConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DbConfigBuilder {
    pub const fn new() -> Self {
        Self {
            block_size: 4096,
            block_sanity_size: 4,
            compressor_type: CompressorType::LZ4,
            block_sanity: BlockSanity::XxH32Checksum,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
            overflow_pg_free_space: [0; 9],
        }
    }

    pub const fn block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }

    pub const fn block_sanity(mut self, block_sanity: BlockSanity) -> Self {
        self.block_sanity = block_sanity;
        self.block_sanity_size = BlockSanity::get_bytes_used(block_sanity);
        self
    }

    pub const fn block_sanity_size(mut self, block_sanity_size: usize) -> Self {
        assert!(block_sanity_size >= BlockSanity::get_bytes_used(self.block_sanity));
        self.block_sanity_size = block_sanity_size;
        self
    }

    pub const fn compressor_type(mut self, compressor_type: CompressorType) -> Self {
        self.compressor_type = compressor_type;
        self
    }

    pub const fn leaf_page_blk_exp(mut self, leaf_page_blk_exp: u8) -> Self {
        self.leaf_page_blk_exp = leaf_page_blk_exp;
        self
    }

    pub const fn dir_page_blk_exp(mut self, dir_page_blk_exp: u8) -> Self {
        self.dir_page_blk_exp = dir_page_blk_exp;
        self
    }

    pub const fn build(mut self) -> DbConfig {
        self.overflow_pg_free_space[0] = (self.block_size * (1 << 0)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[1] = (self.block_size * (1 << 1)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[2] = (self.block_size * (1 << 2)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[3] = (self.block_size * (1 << 3)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[4] = (self.block_size * (1 << 4)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[5] = (self.block_size * (1 << 5)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[6] = (self.block_size * (1 << 6)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[7] = (self.block_size * (1 << 7)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[8] = (self.block_size * (1 << 8)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        
        DbConfig {
            block_size: self.block_size,
            page_size: self.block_size - self.block_sanity_size,
            block_sanity_size: self.block_sanity_size,
            compressor_type: self.compressor_type,
            block_sanity: self.block_sanity,
            leaf_page_blk_exp: self.leaf_page_blk_exp,
            dir_page_blk_exp: self.dir_page_blk_exp,
            overflow_pg_free_space: self.overflow_pg_free_space,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_db_config_builder() {
        let config = DbConfig::builder()
            .block_size(8192)
            .block_sanity_size(4)
            .compressor_type(CompressorType::LZ4)
            .leaf_page_blk_exp(1)
            .dir_page_blk_exp(2)
            .build();

        assert_eq!(config.block_size, 8192);
        assert_eq!(config.page_size, 8188);
        assert_eq!(config.block_sanity_size, 4);
        assert!(matches!(config.compressor_type, CompressorType::LZ4));
        assert_eq!(config.leaf_page_blk_exp, 1);
        assert_eq!(config.dir_page_blk_exp, 2);
    }
}
