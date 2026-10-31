use crate::page::PageType;
use crate::page_no::PageNo;

pub struct SlabRef {
    page_no: PageNo,
    pages_used: u16,
    first_parity: u8,
    next_parity: u8,
}

impl SlabRef {
    pub fn new(page_no: PageNo) -> Self {
        let parity: u8 = rand::random();
        SlabRef {
            page_no,
            pages_used: 0,
            first_parity: parity,
            next_parity: parity,
        }
    }

    pub fn has_free_space(&self) -> bool {
        (self.pages_used as u64) < self.page_no.get_blk_cnt()
    }

    pub fn get_next_page_in_slab(&mut self, page_type: PageType) -> PageNo {
        assert!((self.pages_used as u64) < self.page_no.get_blk_cnt());
        let offset = self.page_no.get_blk_offset() + self.pages_used as u64;
        let parity = self.next_parity;
        self.pages_used += 1;
        self.next_parity = self.next_parity.wrapping_add(1);
        let mut new_page_now = PageNo::new(page_type, 0, offset);
        new_page_now.set_pg_parity(parity);
        new_page_now
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        SlabRef {
            page_no: PageNo::from_bytes(&bytes[..8]),
            pages_used: u16::from_le_bytes(
                bytes[8..10]
                    .try_into()
                    .expect("slice with incorrect length"),
            ),
            first_parity: bytes[10],
            next_parity: bytes[11],
        }
    }

    pub fn to_bytes(&self, bytes: &mut [u8]) {
        bytes[..8].copy_from_slice(&self.page_no.get_bytes());
        bytes[8..10].copy_from_slice(&self.pages_used.to_le_bytes());
        bytes[10] = self.first_parity;
        bytes[11] = self.next_parity;
    }

    pub fn get_page_no(&self) -> PageNo {
        self.page_no
    }

    pub fn get_pages_used(&self) -> u16 {
        self.pages_used
    }

    pub fn set_pages_used(&mut self, pages_used: u16) {
        assert!((pages_used as u64) <= self.page_no.get_blk_cnt());
        self.pages_used = pages_used;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PageType;

    #[test]
    fn slab_ref_test_serialise() {
        let mut slab_ref = SlabRef::new(PageNo::new(PageType::Null, 4, 8));
        slab_ref.set_pages_used(4);
        let mut buffer = [0u8; 12];
        slab_ref.to_bytes(&mut buffer);
        let new_slab_ref = SlabRef::from_bytes(&buffer);
        assert_eq!(slab_ref.get_page_no(), new_slab_ref.get_page_no());
        assert_eq!(slab_ref.get_pages_used(), new_slab_ref.get_pages_used());
    }

    #[test]
    fn slab_ref_test_get_next_page_in_slab() {
        let mut slab_ref = SlabRef::new(PageNo::new(PageType::Slab, 2, 100));
        let first_parity = slab_ref.first_parity;

        for i in 0..4u64 {
            assert!(slab_ref.has_free_space());
            let page_no = slab_ref.get_next_page_in_slab(PageType::LeafPage);
            assert_eq!(page_no.get_pg_type(), PageType::LeafPage);
            assert_eq!(page_no.get_blk_cnt(), 1);
            assert_eq!(page_no.get_blk_offset(), 100 + i);
            assert_eq!(page_no.get_pg_parity(), first_parity.wrapping_add(i as u8));
            assert_eq!(slab_ref.get_pages_used() as u64, i + 1);
        }
        assert!(!slab_ref.has_free_space());
        assert_eq!(slab_ref.first_parity, first_parity);
    }

    #[test]
    fn slab_ref_test_get_next_page_in_slab_parity_wraps() {
        let mut buffer = [0u8; 12];
        SlabRef::new(PageNo::new(PageType::Slab, 1, 8)).to_bytes(&mut buffer);
        buffer[11] = 255;
        let mut slab_ref = SlabRef::from_bytes(&buffer);

        let page_no = slab_ref.get_next_page_in_slab(PageType::DirPage);
        assert_eq!(page_no.get_pg_parity(), 255);
        let page_no = slab_ref.get_next_page_in_slab(PageType::DirPage);
        assert_eq!(page_no.get_pg_parity(), 0);
    }

    #[test]
    fn slab_ref_test_get_next_page_in_slab_after_serialise() {
        let mut slab_ref = SlabRef::new(PageNo::new(PageType::Slab, 2, 40));
        let first = slab_ref.get_next_page_in_slab(PageType::LeafPage);

        let mut buffer = [0u8; 12];
        slab_ref.to_bytes(&mut buffer);
        let mut new_slab_ref = SlabRef::from_bytes(&buffer);

        let second = new_slab_ref.get_next_page_in_slab(PageType::LeafPage);
        assert_eq!(second.get_blk_offset(), first.get_blk_offset() + 1);
        assert_eq!(
            second.get_pg_parity(),
            first.get_pg_parity().wrapping_add(1)
        );
        assert_eq!(new_slab_ref.get_pages_used(), 2);
    }

    #[test]
    #[should_panic]
    fn slab_ref_test_get_next_page_in_full_slab() {
        let mut slab_ref = SlabRef::new(PageNo::new(PageType::Slab, 0, 16));
        slab_ref.get_next_page_in_slab(PageType::LeafPage);
        slab_ref.get_next_page_in_slab(PageType::LeafPage);
    }
}
