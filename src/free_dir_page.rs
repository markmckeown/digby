use crate::db_config::DbConfig;
use crate::page::Page;
use crate::page::PageTrait;
use crate::page_no::PageNo;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

// There needs to be some mechanism to manage free pages in the
// DB. This can be free pages that have been created for
// future writes or pages that were being used in the trees
// that are no longer needed.
//
// A FreeDirPage is page with a list of page numbers. Each
// page number references a free page in the system that can
// be used in the future for the tree.
//
// A single page can only store a limited number of free pages
// which might not be enough. To address this the
// FreeDirPage are arranged as a doubly linked list - if there are
// two FreeDirPage then one is the head and it points to the tail
// page.
//
// To keep track of this linked list the master page stores
// the page number of the head of linked list.
//
// During a change to the DB pages are removed from the free
// page directory and returned to the directory - when
// finalising the update the free page directory (the head
// of the linked list) is updated in the master page.
//
// The free page directory, the linked list of FreeDirPage,
// is treated as a stack. Free pages are added or removed
// from the head FreeDirPage page. If the head FreeDirPage
// fills up then we add another FreeDirPage. If the
// the head becomes empty then we remove it from the linked list
// and add it to the set of free pages.
//
// Management of free pages is done via the FreePageTracker.
//
// Header size 34 bytes
// | Page No (u64) | VersionHolder (8 bytes) | NextPage(u64) | PreviousPage (u64)
// | Entries u16 |
// | Free Page Id (u64) | Free Page Id (u64) |....|
pub struct FreeDirPage {
    page: Page,
}

impl PageTrait for FreeDirPage {
    fn get_page_bytes(&self) -> &[u8] {
        self.page.get_page_bytes()
    }

    fn get_page_number(&self) -> PageNo {
        self.page.get_page_number()
    }

    fn set_page_number(&mut self, page_no: PageNo) {
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

impl FreeDirPage {
    const HEADER_SIZE: usize = 34;
    pub fn create_new(page_config: &DbConfig, page_number: PageNo, version: u64) -> Self {
        FreeDirPage::new(
            page_config.block_size,
            page_config.page_size,
            page_number,
            version,
        )
    }

    fn new(block_size: usize, page_size: usize, page_number: PageNo, version: u64) -> Self {
        let mut free_page_dir = FreeDirPage {
            page: Page::new(block_size, page_size),
        };
        free_page_dir.page.set_type(crate::page::PageType::FreeDir);
        free_page_dir.page.set_page_number(page_number);
        free_page_dir.page.set_version(version);
        free_page_dir
    }

    pub fn from_page(page: Page) -> Self {
        let page_type = page.get_type();
        if page_type != crate::page::PageType::FreeDir {
            panic!("Invalid page type for FreePageDir");
        }

        FreeDirPage { page }
    }

    pub fn get_entries(&self) -> u16 {
        let mut cursor = Cursor::new(self.page.get_page_bytes());
        cursor.set_position(32);
        cursor.read_u16::<LittleEndian>().unwrap()
    }

    pub fn set_entries(&mut self, entries: u16) {
        let mut cursor = Cursor::new(&mut self.page.get_page_bytes_mut()[..]);
        cursor.set_position(32);
        cursor
            .write_u16::<LittleEndian>(entries)
            .expect("Failed to write entries");
    }

    // The next free page directory in the linked list of free page directories.
    pub fn get_nxt_free_dir_pg(&self) -> PageNo {
        PageNo::from_bytes(&self.page.get_page_bytes()[16..16 + 8])
    }

    // Set the next free page directory in the linked list of free page directories.
    pub fn set_nxt_free_dir_pg(&mut self, nxt_free_dir_pg: &PageNo) {
        self.page.get_page_bytes_mut()[16..16 + 8].copy_from_slice(&nxt_free_dir_pg.get_bytes());
    }

    // The previous free page directory in the linked list of free page directories.
    pub fn set_prev_free_dir_pg(&mut self, prev_free_dir_pg: &PageNo) {
        self.page.get_page_bytes_mut()[24..24 + 8].copy_from_slice(&prev_free_dir_pg.get_bytes());
    }

    fn is_full_for(&self, number_of_pages: usize) -> bool {
        let capacity = self.page.get_page_bytes().len() - FreeDirPage::HEADER_SIZE;
        (capacity - (8 * self.get_entries() as usize)) < 8 * number_of_pages
    }

    pub fn is_full(&self) -> bool {
        self.is_full_for(1)
    }

    pub fn has_free_pages(&self) -> bool {
        self.get_entries() > 0
    }

    pub fn get_free_page(&mut self) -> PageNo {
        assert!(self.has_free_pages());
        let entries = self.get_entries() - 1;
        self.set_entries(entries);
        let offset = FreeDirPage::HEADER_SIZE + (8 * entries as usize);
        PageNo::from_bytes(&self.page.get_page_bytes_mut()[offset..offset + 8])
    }

    pub fn add_free_page(&mut self, free_page_number: PageNo) {
        assert!(!self.is_full());
        let entries = self.get_entries();
        let offset = FreeDirPage::HEADER_SIZE + (8 * self.get_entries() as usize);
        self.page.get_page_bytes_mut()[offset..offset + 8]
            .copy_from_slice(&free_page_number.get_bytes());
        self.set_entries(entries + 1);
    }

    pub fn add_free_pages(&mut self, free_pages: &Vec<PageNo>) {
        assert!(!self.is_full_for(free_pages.len()));
        assert!(free_pages.len() < u16::MAX as usize);
        let entries = self.get_entries();
        let mut offset = FreeDirPage::HEADER_SIZE + (8 * self.get_entries() as usize);
        for free_page in free_pages {
            self.page.get_page_bytes_mut()[offset..offset + 8]
                .copy_from_slice(&free_page.get_bytes());
            offset += 8;
        }
        self.set_entries(entries + free_pages.len() as u16);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adding_entries() {
        let mut free_page_dir = FreeDirPage::new(4096, 4092, PageNo::new(0, 34), 4564);
        assert!(!free_page_dir.has_free_pages());
        free_page_dir.add_free_page(PageNo::new(0, 73));
        free_page_dir.add_free_page(PageNo::new(0, 103));
        assert_eq!(4092, free_page_dir.get_page_bytes().len());
        assert!(free_page_dir.has_free_pages());
        assert!(103 == free_page_dir.get_free_page().get_blk_offset());
        assert!(73 == free_page_dir.get_free_page().get_blk_offset());
        assert!(!free_page_dir.has_free_pages());
    }

    #[test]
    fn test_fill_free_page_dir() {
        let mut free_page_dir = FreeDirPage::new(4096, 4092, PageNo::new(0, 34), 657);
        let mut count = 0;
        for number in 1..=1020 {
            if !free_page_dir.is_full() {
                count += 1;
                free_page_dir.add_free_page(PageNo::new(0, number));
            }
        }
        assert!(free_page_dir.is_full());
        assert_eq!(count, 507);
        assert_eq!(507, free_page_dir.get_free_page().get_blk_offset());
        assert!(!free_page_dir.is_full());
    }

    #[test]
    fn test_invalid_type() {
        let mut free_page_dir = FreeDirPage::new(4096, 4092, PageNo::new(0, 34), 657);
        free_page_dir.page.set_type(crate::page::PageType::DbMaster);
        let result = std::panic::catch_unwind(|| FreeDirPage::from_page(free_page_dir.page));
        assert!(result.is_err());
    }
}
