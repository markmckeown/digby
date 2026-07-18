use crate::compressor::CompressorType;

#[derive(Copy, Clone, Debug)]
pub struct DbConfig {
    pub block_size: usize,
    pub page_size: usize,
    pub block_sanity_size: usize,
    pub compressor_type: CompressorType,
    pub leaf_page_blk_exp: u8,
    pub dir_page_blk_exp: u8,
}

impl DbConfig {
    pub fn builder() -> DbConfigBuilder {
        DbConfigBuilder::default()
    }
}

#[derive(Clone, Debug)]
pub struct DbConfigBuilder {
    block_size: usize,
    page_size: usize,
    block_sanity_size: usize,
    compressor_type: CompressorType,
    leaf_page_blk_exp: u8,
    dir_page_blk_exp: u8,
}

impl Default for DbConfigBuilder {
    fn default() -> Self {
        Self {
            block_size: 4096,
            page_size: 4096,
            block_sanity_size: 0,
            compressor_type: CompressorType::LZ4,
            leaf_page_blk_exp: 0,
            dir_page_blk_exp: 0,
        }
    }
}

impl DbConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }

    pub fn page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size;
        self
    }

    pub fn block_sanity_size(mut self, block_sanity_size: usize) -> Self {
        self.block_sanity_size = block_sanity_size;
        self
    }

    pub fn compressor_type(mut self, compressor_type: CompressorType) -> Self {
        self.compressor_type = compressor_type;
        self
    }

    pub fn leaf_page_blk_exp(mut self, leaf_page_blk_exp: u8) -> Self {
        self.leaf_page_blk_exp = leaf_page_blk_exp;
        self
    }

    pub fn dir_page_blk_exp(mut self, dir_page_blk_exp: u8) -> Self {
        self.dir_page_blk_exp = dir_page_blk_exp;
        self
    }

    pub fn build(self) -> DbConfig {
        DbConfig {
            block_size: self.block_size,
            page_size: self.page_size,
            block_sanity_size: self.block_sanity_size,
            compressor_type: self.compressor_type,
            leaf_page_blk_exp: self.leaf_page_blk_exp,
            dir_page_blk_exp: self.dir_page_blk_exp,
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
            .page_size(8188)
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
