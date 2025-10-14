use crate::page::PageTrait;
use crate::{FreePageTracker, TreeDirEntry};
use crate::page_cache::PageCache;
use crate::tree_dir_page::TreeDirPage;

pub struct TreeDirHandler {

}

pub struct TreeDirPageRef {
    pub page: TreeDirPage,
    pub left_key: Option<Vec<u8>>,
}

impl TreeDirHandler {
    pub fn handle_tree_leaf_store(
            mut tree_dir_page: TreeDirPage, 
            entries: Vec<TreeDirEntry>,
            page_size: usize) -> Vec<TreeDirPageRef> {
        assert!(!entries.is_empty(), "entries was empty");
        let mut tree_dir_pages: Vec<TreeDirPageRef> = Vec::new();

        /*
        let mut entries: Vec<TreeDirEntry> = Vec::new();
        for leaf_page in leaf_pages {
            let tree_dir_entry = TreeDirEntry::new(leaf_page.get_left_key(page_size).unwrap(), leaf_page.get_page_number());
            entries.push(tree_dir_entry);
        }
        */

        // Do not need to split the tree dir page.
        if tree_dir_page.can_fit_entries(&entries) {
            tree_dir_page.add_entries(entries, page_size);
            tree_dir_pages.push(
                TreeDirPageRef { 
                    page: tree_dir_page, 
                    left_key: None 
                });
            return tree_dir_pages;
        }

        // Need to split the tree dir page.
        let entries_to_right = tree_dir_page.get_right_half_entries(page_size);
        assert!(!entries_to_right.is_empty());
        let new_left_key = entries_to_right.get(0).unwrap().get_key().to_vec();
        let mut new_tree_dir_page = TreeDirPage::new(page_size as u64, 0, 0);
        new_tree_dir_page.add_split_entries_new_page(entries_to_right, page_size);


        if entries.get(0).unwrap().get_key() < new_left_key.as_ref() {
            // Use original page to add entries. Note if the first is less than the left key in the
            // new page then all entries will be.
            tree_dir_page.add_entries(entries, page_size);
        } else {
            // Use the original page.
            new_tree_dir_page.add_entries(entries, page_size);
        }
        tree_dir_pages.push(TreeDirPageRef{ page: tree_dir_page, left_key: None});
        tree_dir_pages.push(TreeDirPageRef{ page: new_tree_dir_page, left_key: Some(new_left_key)});

        return tree_dir_pages;
    }


    pub fn map_pages(page_refs: &mut Vec<TreeDirPageRef>, 
                    free_page_tracker: &mut FreePageTracker, 
                    page_cache: &mut PageCache, 
                    version: u64) -> () {
        for page_ref in page_refs {
            let old_page_no = page_ref.page.get_page_number();
            if old_page_no != 0 {
                free_page_tracker.return_free_page_no(old_page_no);
            }
            let new_page_no = free_page_tracker.get_free_page(page_cache);
            page_ref.page.set_page_number(new_page_no);
            page_ref.page.set_version(version);
        }
    }


    pub fn handle_tree_dir_store(
            mut parent_dir_page: TreeDirPage, 
            entries: Vec<TreeDirEntry>, 
            version: u64,
            page_size: usize) -> Vec<TreeDirPageRef> {
        assert!(!entries.is_empty(), "dir entries was empty");
        let mut tree_dir_pages: Vec<TreeDirPageRef> = Vec::new();


        // Do not need to split the tree dir page.
        if parent_dir_page.can_fit_entries(&entries) {
            parent_dir_page.add_entries(entries, page_size);
            tree_dir_pages.push(TreeDirPageRef { 
                    page: parent_dir_page, 
                    left_key: None 
                });
            return tree_dir_pages;
        }

        // Need to split the parent dir page.
        let entries_to_right = parent_dir_page.get_right_half_entries(page_size);
        assert!(!entries_to_right.is_empty());
        let new_page_left_key = entries_to_right.get(0).unwrap().get_key().to_vec();
        let mut new_tree_page = TreeDirPage::new(page_size as u64, 0, version);
        new_tree_page.add_split_entries_new_page(entries_to_right, page_size);

        if entries.get(0).unwrap().get_key() < new_page_left_key.as_ref() {
            // Add entries to original page.
            parent_dir_page.add_entries(entries, page_size);
        } else {
            new_tree_page.add_entries(entries, page_size);
        }
        tree_dir_pages.push(
            TreeDirPageRef{
                page: parent_dir_page,
                left_key: None,
        });
        tree_dir_pages.push(
            TreeDirPageRef{
                page: new_tree_page,
                left_key: Some(new_page_left_key),
        });

        return tree_dir_pages;
    }

}

#[cfg(test)]
mod tests {
    use crate::Tuple;
    use crate::TreeLeafPage;

    use super::*;

    #[test]
    fn test_add_1() {
        let page_size: usize = 4096;
        let mut tree_leaf_page = TreeLeafPage::new(page_size as u64, 0);
        tree_leaf_page.set_page_number(21);
        let tuple: Tuple = Tuple::new(b"f".to_vec().as_ref(), b"f_value".to_vec().as_ref(), 345);
        tree_leaf_page.store_tuple(tuple, page_size);
        
        let mut tree_leaf_page1 = TreeLeafPage::new(page_size as u64, 0);
        tree_leaf_page1.set_page_number(27);
        let tuple1: Tuple = Tuple::new(b"h".to_vec().as_ref(), b"h_value".to_vec().as_ref(), 345);
        tree_leaf_page1.store_tuple(tuple1, page_size);

        
        let mut leaf_pages: Vec<TreeLeafPage> = Vec::new();
        leaf_pages.push(tree_leaf_page);
        leaf_pages.push(tree_leaf_page1);

        let mut entries: Vec<TreeDirEntry> = Vec::new();
        for leaf_page in leaf_pages {
            let tree_dir_entry = TreeDirEntry::new(leaf_page.get_left_key(page_size).unwrap(), leaf_page.get_page_number());
            entries.push(tree_dir_entry);
        }

                
        let mut tree_dir_page = TreeDirPage::new(page_size as u64, 0, 0);

        let mut new_pages = TreeDirHandler::handle_tree_leaf_store(tree_dir_page, entries, page_size);
        assert_eq!(new_pages.len(), 1);
        tree_dir_page = new_pages.pop().unwrap().page;
        assert_eq!(tree_dir_page.get_page_to_left(), 21);
        assert_eq!(tree_dir_page.get_dir_left_key(page_size).unwrap(), b"h".to_vec());

        let tuple3: Tuple = Tuple::new(b"a".to_vec().as_ref(), b"a_value".to_vec().as_ref(), 345);
        tree_leaf_page = TreeLeafPage::new(page_size as u64, 0);
        tree_leaf_page.store_tuple(tuple3, page_size);
        tree_leaf_page.set_page_number(79);
        leaf_pages = Vec::new();
        leaf_pages.push(tree_leaf_page);

        entries = Vec::new();
        for leaf_page in leaf_pages {
            let tree_dir_entry = TreeDirEntry::new(leaf_page.get_left_key(page_size).unwrap(), leaf_page.get_page_number());
            entries.push(tree_dir_entry);
        }

        new_pages = TreeDirHandler::handle_tree_leaf_store(tree_dir_page, entries, page_size);
        assert_eq!(new_pages.len(), 1);
        tree_dir_page = new_pages.pop().unwrap().page;
        assert_eq!(tree_dir_page.get_page_to_left(), 79);
    }

}