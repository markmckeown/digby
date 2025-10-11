use crate::page::PageTrait;
use crate::{FreePageTracker, TreeDirEntry, TreeInternalPage, TreeLeafPage};
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
            mut leaf_pages: Vec<TreeLeafPage>,
            page_size: usize) -> Vec<TreeDirPageRef> {
        assert!(!leaf_pages.is_empty(), "leaf_pages was empty");
        let mut tree_dir_pages: Vec<TreeDirPageRef> = Vec::new();

        // Sort the leaf pages and build a set of TreeDirEntry.
        leaf_pages.sort_by(|b, a| b.get_left_key(page_size).unwrap().cmp(&a.get_left_key(page_size).unwrap()));
        let mut entries: Vec<TreeDirEntry> = Vec::new();
        for leaf_page in leaf_pages {
            let tree_dir_entry = TreeDirEntry::new(leaf_page.get_left_key(page_size).unwrap(), leaf_page.get_page_number());
            entries.push(tree_dir_entry);
        }

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


    pub fn map_pages(pages: &mut Vec<TreeInternalPage>, 
                    free_page_tracker: &mut FreePageTracker, 
                    page_cache: &mut PageCache, 
                    version: u64) -> () {
        for page in pages {
            let old_page_no = page.get_page_number();
            if old_page_no != 0 {
                free_page_tracker.return_free_page_no(old_page_no);
            }
            let new_page_no = free_page_tracker.get_free_page(page_cache);
            page.set_page_number(new_page_no);
            page.set_version(version);
        }
    }


    pub fn handle_tree_dir_store(
            mut parent_dir_page: TreeDirPage, 
            dir_pages: Vec<TreeDirPageRef>, 
            version: u64,
            page_size: usize) -> Vec<TreeDirPageRef> {
        assert!(!dir_pages.is_empty(), "leaf_pages was empty");
        let mut tree_dir_pages: Vec<TreeDirPageRef> = Vec::new();

        let mut entries: Vec<TreeDirEntry> = Vec::new();
        for dir_page_ref in dir_pages {
            let tree_dir_entry: TreeDirEntry;
            if dir_page_ref.left_key.is_none() {
               tree_dir_entry = TreeDirEntry::new(
                dir_page_ref.page.get_dir_left_key(page_size).unwrap(), 
                dir_page_ref.page.get_page_number());
            } else {
                tree_dir_entry = TreeDirEntry::new(
                dir_page_ref.left_key.unwrap(), 
                dir_page_ref.page.get_page_number());
            }
            entries.push(tree_dir_entry);
        }

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

