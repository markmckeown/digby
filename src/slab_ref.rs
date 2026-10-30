use crate::page_no::PageNo;

// Two extra bytes
pub struct SlabRef {
    page_no: PageNo,
    pages_used: u16,
    unused_1: u8,
    unused_2: u8,
}

impl SlabRef {
    pub fn new(page_no: PageNo) -> Self {
        SlabRef {
            page_no,
            pages_used: 0,
            unused_1: 0,
            unused_2: 0,
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        SlabRef {
            page_no: PageNo::from_bytes(&bytes[..8]),
            pages_used: u16::from_le_bytes(
                bytes[8..10]
                    .try_into()
                    .expect("slice with incorrect length"),
            ),
            unused_1: bytes[10],
            unused_2: bytes[11],
        }
    }

    pub fn to_bytes(&self, bytes: &mut [u8]) {
        bytes[..8].copy_from_slice(&self.page_no.get_bytes());
        bytes[8..10].copy_from_slice(&self.pages_used.to_le_bytes());
        bytes[10] = self.unused_1;
        bytes[11] = self.unused_2;
    }

    pub fn get_page_no(&self) -> PageNo {
        self.page_no
    }

    pub fn get_pages_used(&self) -> u16 {
        self.pages_used
    }

    pub fn set_pages_used(&mut self, pages_used: u16) {
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
        slab_ref.set_pages_used(45);
        let mut buffer = [0u8; 12];
        slab_ref.to_bytes(&mut buffer);
        let new_slab_ref = SlabRef::from_bytes(&buffer);
        assert_eq!(slab_ref.get_page_no(), new_slab_ref.get_page_no());
        assert_eq!(slab_ref.get_pages_used(), new_slab_ref.get_pages_used());
    }
}
