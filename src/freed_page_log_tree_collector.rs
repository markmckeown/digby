use crate::page_no::PageNo;

pub struct FreedPageLogTreeCollector {
    pages: Vec<PageNo>,
}

impl FreedPageLogTreeCollector {
    pub fn new() -> Self {
        Self { pages: Vec::new() }
    }

    pub fn add(&mut self, page_no: PageNo) {
        self.pages.push(page_no);
    }

    pub fn get_pages_buffer(&self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::new();
        for page_no in &self.pages {
            buffer.extend_from_slice(&page_no.get_bytes());
        }
        buffer
    }
}

impl Default for FreedPageLogTreeCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PageType;

    #[test]
    fn test_empty_collector() {
        let collector = FreedPageLogTreeCollector::new();
        assert!(collector.get_pages_buffer().is_empty());
    }

    #[test]
    fn test_single_page() {
        let mut collector = FreedPageLogTreeCollector::new();
        let page_no = PageNo::new(PageType::LeafPage, 1, 42);
        collector.add(page_no);
        assert_eq!(collector.get_pages_buffer(), page_no.get_bytes().to_vec());
    }

    #[test]
    fn test_multiple_pages_in_order() {
        let mut collector = FreedPageLogTreeCollector::new();
        let mut parity_page = PageNo::new(PageType::Overflow, 3, 1000);
        parity_page.set_pg_parity(200);
        let page_nos = [
            PageNo::new(PageType::LeafPage, 0, 7),
            PageNo::new(PageType::DirPage, 2, 3),
            parity_page,
            // Duplicates are kept.
            PageNo::new(PageType::LeafPage, 0, 7),
        ];
        for page_no in page_nos {
            collector.add(page_no);
        }

        let buffer = collector.get_pages_buffer();
        assert_eq!(buffer.len(), page_nos.len() * 8);
        let read_page_nos: Vec<PageNo> = buffer.chunks(8).map(PageNo::from_bytes).collect();
        assert_eq!(read_page_nos, page_nos.to_vec());
    }

    #[test]
    fn test_get_pages_buffer_is_repeatable() {
        let mut collector = FreedPageLogTreeCollector::new();
        collector.add(PageNo::new(PageType::LeafPage, 0, 11));
        let first = collector.get_pages_buffer();
        assert_eq!(collector.get_pages_buffer(), first);

        collector.add(PageNo::new(PageType::LeafPage, 0, 12));
        let second = collector.get_pages_buffer();
        assert_eq!(&second[..8], &first[..]);
        assert_eq!(
            PageNo::from_bytes(&second[8..]),
            PageNo::new(PageType::LeafPage, 0, 12)
        );
    }
}
