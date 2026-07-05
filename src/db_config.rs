#[derive(Copy, Clone)]
pub struct DbConfig {
    pub block_size: usize,
    pub page_size: usize,
    pub block_sanity_size: usize,
}
