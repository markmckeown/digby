use crate::page::PageTrait;
use crate::{FreePageTracker, TreeDirEntry, TreeInternalPage, TreeLeafPage};
use crate::page_cache::PageCache;

pub struct TreeDirHandler {

}

impl TreeDirHandler {
    pub fn handle_update_tree_leaf(
            mut tree_leaf_page: TreeInternalPage, 
            mut leaf_pages: Vec<TreeLeafPage>, 
            _free_page_tracker: &mut FreePageTracker,
            _page_cache: &mut PageCache,
            version: u64,
            page_size: usize) -> Vec<TreeInternalPage> {
        assert!(!leaf_pages.is_empty(), "leaf_pages was empty");
        let mut tree_dir_pages: Vec<TreeInternalPage> = Vec::new();

        // Sort the leaf pages and build a set of TreeDirEntry.
        leaf_pages.sort_by(|b, a| b.get_left_key(page_size).unwrap().cmp(&a.get_left_key(page_size).unwrap()));
        let mut entries: Vec<TreeDirEntry> = Vec::new();
        for leaf_page in leaf_pages {
            let tree_dir_entry = TreeDirEntry::new(leaf_page.get_left_key(page_size).unwrap(), leaf_page.get_page_number());
            entries.push(tree_dir_entry);
        }

        // Do not need to split the tree dir page.
        if tree_leaf_page.can_fit_entries(&entries) {
            tree_leaf_page.add_entries(entries, page_size);
            tree_dir_pages.push(tree_leaf_page);
            return tree_dir_pages;
        }

        // Need to split the tree dir page.
        let entries_to_right = tree_leaf_page.get_right_half_entries(page_size);
        assert!(!entries_to_right.is_empty());
        let use_original_page: bool = entries.get(0).unwrap().get_key() < entries_to_right.get(0).unwrap().get_key();
        let mut new_tree_page = TreeInternalPage::new(page_size as u64, 0, version);
        new_tree_page.set_page_to_left(entries_to_right.get(0).unwrap().get_page_no());
        // Do NOT iterate from 0, we skip first dir entry as we added it as the page to the left
        for i in 1..entries_to_right.len() {
           new_tree_page.add_tree_dir_entry(entries_to_right.get(i).unwrap(), page_size as u64);
        } 

        if use_original_page {
            tree_leaf_page.add_entries(entries, page_size);
        } else {
            new_tree_page.add_entries(entries, page_size);
        }
        tree_dir_pages.push(tree_leaf_page);
        tree_dir_pages.push(new_tree_page);

        return tree_dir_pages;
    }
}