pub struct LeafPage {
    page: Page,
}

impl PageTrait for LeafPage {
    fn get_page_bytes(&self) -> &[u8] {
        self.page.get_page_bytes()
    }

    fn get_page_number(&self) -> u64 {
        self.page.get_page_number()
    }

    fn set_page_number(&mut self, page_no: u64) {
        self.page.set_page_number(page_no)
    }

    fn get_page(&mut self) -> &mut Page {
        &mut self.page
    }

    fn get_version(&self) -> u64 {
        self.page.get_version()
    }

    fn set_version(&mut self, version: u64) {
        self.page.set_version(version);
    }
}

pub struct Slot {
    offset: u16,
    key_len: u8,
    val_len: u16
}


// Header
// | Page No (8 bytes) | VersionHolder(8 bytes) | Entries(u16) | Free_Space(u16) |
// | prefix_length (u8) | slot | slot | slot | ...
// | heap
// | key | value | key | value |

impl LeafPage {
    const HEADER_SIZE: usize = 21; // 8 + 8 + 2 + 2 + 1
    const SLOT_SIZE: usize = 5; // 2 + 1 + 2

    pub fn create_new(page_config: &PageConfig, page_number: u64) -> Self {
        LeafPage::new{ page: Page::new(page_config.block_size, page_config.page_size, page_number) }
    }

    fn new(block_size: usize, page_size: usize, page_number: u64) -> Self {
        let mut page = Page::new(block_size, page_size);
        page.set_type(PageType::Leaf);
        page.set_page_number(page_number);
        let mut leaf_page = LeafPage { page };
        leaf_page
    }


    pub fn from_page(page: Page) -> Self {
        if page.get_type() != PageType::Leaf {
            panic!("Page type is not Leaf");
        }
        LeafPage { page }
    }

    pub fn get_entries(&self) -> u16 {
        let bytes = &self.page.get_page_bytes()[16..18];
        u16::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn set_entries(&mut self, entries: u16) {
        let bytes = entries.to_le_bytes();
        self.page.get_page_bytes_mut()[16..18].copy_from_slice(&bytes);
    }

    pub fn get_free_space(&self) -> u16 {
        let bytes = &self.page.get_page_bytes()[18..20];
        u16::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn set_free_space(&mut self, free_space: u16) {
        let bytes = free_space.to_le_bytes();
        self.page.get_page_bytes_mut()[18..20].copy_from_slice(&bytes);
    }

    pub fn get_prefix_length(&self) -> u8 {
        self.page.get_page_bytes()[20]
    }

    pub fn set_prefix_length(&mut self, prefix_length: u8) {
        self.page.get_page_bytes_mut()[20] = prefix_length;
    }
    
    pub fn get_slot_at_index(&self, index: usize) -> Slot {
        let slot_offset = LeafPage::HEADER_SIZE + index * LeafPage::SLOT_SIZE;
        let offset_bytes = &self.page.get_page_bytes()[slot_offset..slot_offset + 2];
        let offset = u16::from_le_bytes(offset_bytes.try_into().unwrap());
        let key_len = self.page.get_page_bytes()[slot_offset + 2];
        let val_len_bytes = &self.page.get_page_bytes()[slot_offset + 3..slot_offset + 5];
        let val_len = u16::from_le_bytes(val_len_bytes.try_into().unwrap());
        Slot { offset, key_len, val_len }
    }

    pub fn set_slot_at_index(&mut self, index: usize, slot: Slot) {
        let slot_offset = LeafPage::HEADER_SIZE + index * LeafPage::SLOT_SIZE;
        let offset_bytes = slot.offset.to_le_bytes();
        self.page.get_page_bytes_mut()[slot_offset..slot_offset + 2].copy_from_slice(&offset_bytes);
        self.page.get_page_bytes_mut()[slot_offset + 2] = slot.key_len;
        let val_len_bytes = slot.val_len.to_le_bytes();
        self.page.get_page_bytes_mut()[slot_offset + 3..slot_offset + 5].copy_from_slice(&val_len_bytes);
    }

    pub fn get_key_at_slot(&self, slot: Slot) -> [u8] {
        let key_offset = slot.offset as usize;
        self.page.get_page_bytes()[key_offset..key_offset + slot.key_len as usize]
    }

    pub fn get_value_at_slot(&self, slot: Slot) -> [u8] {
        let val_offset = (slot.offset + slot.key_len as u16) as usize;
        self.page.get_page_bytes()[val_offset..val_offset + slot.val_len as usize]
    }

    
    
}