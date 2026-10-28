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
    pub leaf_pg_blk_cnt_shift: u8,
    pub dir_pg_blk_cnt_shift: u8,
    pub max_blk_shift: u8,
    pub overflow_pg_free_space: [usize; 16],
}

impl DbConfig {
    pub const MAX_BLOCK_SHIFT_LIMIT: u8 = 15;
    // Leaf and directory pages store offsets and free space as u16, so
    // their page containers cannot be larger than 64K.
    pub const MAX_TREE_PAGE_SIZE: usize = 64 * 1024;

    pub const fn builder() -> DbConfigBuilder {
        DbConfigBuilder::new()
    }

    pub const fn get_leaf_page_blk_cnt(&self) -> u64 {
        1 << self.leaf_pg_blk_cnt_shift
    }

    pub const fn get_dir_page_blk_cnt(&self) -> u64 {
        1 << self.dir_pg_blk_cnt_shift
    }

    pub fn get_max_overflow_pg_free_space(&self) -> usize {
        self.overflow_pg_free_space[self.max_blk_shift as usize]
    }

    pub fn get_blk_cnt_shift_for_size(&self, size: usize) -> u8 {
        let mut i = 0usize;
        while i <= self.max_blk_shift as usize {
            if size <= self.overflow_pg_free_space[i] {
                return i as u8;
            }
            i += 1;
        }
        panic!("Size is larger than the largest overflow page");
    }

    pub fn get_max_overflow_exp_size(&self) -> u8 {
        self.max_blk_shift
    }
}

#[derive(Clone, Debug)]
pub struct DbConfigBuilder {
    block_size: usize,
    block_sanity_size: usize,
    compressor_type: CompressorType,
    block_sanity: BlockSanity,
    leaf_pg_blk_cnt_shift: u8,
    dir_pg_blk_cnt_shift: u8,
    overflow_pg_free_space: [usize; 16],
    max_blk_shift: u8,
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
            leaf_pg_blk_cnt_shift: 0,
            dir_pg_blk_cnt_shift: 0,
            overflow_pg_free_space: [0; 16],
            max_blk_shift: 8,
        }
    }

    pub const fn max_blk_shift(mut self, max_blk_shift: u8) -> Self {
        self.max_blk_shift = max_blk_shift;
        self
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

    // For testing only - block_sanity_size should be set through setting
    // the block_sanity.
    pub const fn block_sanity_size(mut self, block_sanity_size: usize) -> Self {
        assert!(block_sanity_size >= BlockSanity::get_bytes_used(self.block_sanity));
        self.block_sanity_size = block_sanity_size;
        self
    }

    pub const fn compressor_type(mut self, compressor_type: CompressorType) -> Self {
        self.compressor_type = compressor_type;
        self
    }

    pub const fn leaf_pg_blk_cnt_shift(mut self, leaf_pg_blk_cnt_shift: u8) -> Self {
        self.leaf_pg_blk_cnt_shift = leaf_pg_blk_cnt_shift;
        self
    }

    pub const fn dir_pg_blk_cnt_shift(mut self, dir_pg_blk_cnt_shift: u8) -> Self {
        self.dir_pg_blk_cnt_shift = dir_pg_blk_cnt_shift;
        self
    }

    pub const fn build(mut self) -> DbConfig {
        assert!(
            self.max_blk_shift <= DbConfig::MAX_BLOCK_SHIFT_LIMIT,
            "Max block shift is limited to 15."
        );
        assert!(
            self.leaf_pg_blk_cnt_shift <= self.max_blk_shift
                && (self.block_size << self.leaf_pg_blk_cnt_shift) <= DbConfig::MAX_TREE_PAGE_SIZE,
            "Leaf pages cannot be larger than 64K."
        );
        assert!(
            self.dir_pg_blk_cnt_shift <= self.max_blk_shift
                && (self.block_size << self.dir_pg_blk_cnt_shift) <= DbConfig::MAX_TREE_PAGE_SIZE,
            "Directory pages cannot be larger than 64K."
        );
        self.overflow_pg_free_space[0] =
            self.block_size - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[1] =
            (self.block_size * (1 << 1)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[2] =
            (self.block_size * (1 << 2)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[3] =
            (self.block_size * (1 << 3)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[4] =
            (self.block_size * (1 << 4)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[5] =
            (self.block_size * (1 << 5)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[6] =
            (self.block_size * (1 << 6)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[7] =
            (self.block_size * (1 << 7)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[8] =
            (self.block_size * (1 << 8)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[9] =
            (self.block_size * (1 << 9)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[10] =
            (self.block_size * (1 << 10)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[11] =
            (self.block_size * (1 << 11)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[12] =
            (self.block_size * (1 << 12)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[13] =
            (self.block_size * (1 << 13)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[14] =
            (self.block_size * (1 << 14)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);
        self.overflow_pg_free_space[15] =
            (self.block_size * (1 << 15)) - (OverflowPage::HEADER_SIZE + self.block_sanity_size);

        DbConfig {
            block_size: self.block_size,
            page_size: self.block_size - self.block_sanity_size,
            block_sanity_size: self.block_sanity_size,
            compressor_type: self.compressor_type,
            block_sanity: self.block_sanity,
            leaf_pg_blk_cnt_shift: self.leaf_pg_blk_cnt_shift,
            dir_pg_blk_cnt_shift: self.dir_pg_blk_cnt_shift,
            overflow_pg_free_space: self.overflow_pg_free_space,
            max_blk_shift: self.max_blk_shift,
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
            .leaf_pg_blk_cnt_shift(1)
            .dir_pg_blk_cnt_shift(2)
            .build();

        assert_eq!(config.block_size, 8192);
        assert_eq!(config.page_size, 8188);
        assert_eq!(config.block_sanity_size, 4);
        assert!(matches!(config.compressor_type, CompressorType::LZ4));
        assert_eq!(config.leaf_pg_blk_cnt_shift, 1);
        assert_eq!(config.dir_pg_blk_cnt_shift, 2);
    }

    #[test]
    fn test_blk_cnt_shift_for_size() {
        let config = DbConfig::builder().build();
        assert_eq!(config.get_blk_cnt_shift_for_size(1), 0);
        assert_eq!(
            config.get_blk_cnt_shift_for_size(config.get_max_overflow_pg_free_space()),
            8
        );
    }

    #[test]
    #[should_panic(expected = "Size is larger than the largest overflow page")]
    fn test_blk_cnt_shift_for_size_too_large() {
        let config = DbConfig::builder().build();
        config.get_blk_cnt_shift_for_size(config.get_max_overflow_pg_free_space() + 1);
    }

    #[test]
    fn test_builder_default() {
        assert_eq!(
            format!("{:?}", DbConfigBuilder::default()),
            format!("{:?}", DbConfig::builder())
        );
        let config = DbConfigBuilder::default().build();
        assert_eq!(config.block_size, 4096);
        assert_eq!(config.page_size, 4092);
    }

    #[test]
    fn test_64k_tree_pages() {
        let config = DbConfig::builder()
            .leaf_pg_blk_cnt_shift(4)
            .dir_pg_blk_cnt_shift(4)
            .build();
        assert_eq!(config.block_size << config.leaf_pg_blk_cnt_shift, 64 * 1024);
    }

    #[test]
    #[should_panic(expected = "Leaf pages cannot be larger than 64K.")]
    fn test_leaf_page_over_64k() {
        DbConfig::builder().leaf_pg_blk_cnt_shift(5).build();
    }

    #[test]
    #[should_panic(expected = "Directory pages cannot be larger than 64K.")]
    fn test_dir_page_over_64k() {
        DbConfig::builder().dir_pg_blk_cnt_shift(5).build();
    }

    #[test]
    #[should_panic(expected = "Leaf pages cannot be larger than 64K.")]
    fn test_block_size_over_64k() {
        DbConfig::builder().block_size(128 * 1024).build();
    }

    #[test]
    fn test_max_overflow_page() {
        // 4K blocks, the largest overflow page is 1M.
        let config = DbConfig::builder().build();
        assert_eq!(config.get_max_overflow_exp_size(), 8);
        assert_eq!(
            config.get_max_overflow_pg_free_space(),
            (1 << 20) - (OverflowPage::HEADER_SIZE + config.block_sanity_size)
        );
    }
}
