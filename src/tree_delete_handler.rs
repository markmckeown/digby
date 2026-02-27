use crate::page::{PageTrait, PageType};
use crate::tuple::{Overflow, TupleTrait};
use crate::{
    FreePageTracker, OverflowPageHandler, Page, PageCache, TreeDirEntry, TreeDirPage, TreeLeafPage,
};
pub struct TreeDeleteHandler {}

impl TreeDeleteHandler {
    pub fn delete_key(
        key: &Vec<u8>,
        root_page: Page,
        page_cache: &mut PageCache,
        free_page_tracker: &mut FreePageTracker,
        new_version: u64,
    ) -> (u64, bool) {
        if root_page.get_type() == PageType::TreeLeaf {
            // The root of the tree is actually a leaf page - requires special handling.
            let mut tree_root_single = TreeLeafPage::from_page(root_page);
            return TreeDeleteHandler::delete_key_from_root(
                key,
                &mut tree_root_single,
                page_cache,
                free_page_tracker,
                new_version,
            );
        }

        let root_dir_page = TreeDirPage::from_page(root_page);
        return TreeDeleteHandler::delete_key_from_tree(
            key,
            root_dir_page,
            page_cache,
            free_page_tracker,
            new_version,
        );
    }

    fn delete_key_from_tree(
        key: &Vec<u8>,
        root_dir_page: TreeDirPage,
        page_cache: &mut PageCache,
        free_page_tracker: &mut FreePageTracker,
        new_version: u64,
    ) -> (u64, bool) {
        let root_page_no = root_dir_page.get_page_number();
        let mut dir_page = root_dir_page;
        // This is the stack for storing the tree dir as we descend into
        // the tree.
        let mut dir_pages: Vec<TreeDirPage> = Vec::new();
        let mut next_page: u64;
        let mut leaf_page: TreeLeafPage;
        // loop down until we hit the leaf page keeping a track of the
        // the dir pages as we go.
        loop {
            next_page = dir_page.get_next_page(&key);
            dir_pages.push(dir_page);
            let page = page_cache.get_page(next_page);
            if page.get_type() == PageType::TreeLeaf {
                leaf_page = TreeLeafPage::from_page(page);
                break;
            }
            dir_page = TreeDirPage::from_page(page);
        }

        let tuple = leaf_page.delete_key(key);
        if tuple.is_none() {
            return (root_page_no, false);
        }

        // Have we just removed an overflow page?
        let tuple_unwrapped = tuple.unwrap();
        if tuple_unwrapped.get_overflow() != Overflow::None {
            // Overflow page - need to delete overflows.
            OverflowPageHandler::delete_overflow_tuple_pages(
                Some(tuple_unwrapped),
                page_cache,
                free_page_tracker,
            );
        }

        // Store the root page back into the page cache - should not do this if it is empty!
        let mut new_leaf_page_no: u64 = 0;
        // we always return the leaf page number to be recycled.
        let old_leaf_page_no = leaf_page.get_page_number();
        free_page_tracker.return_free_page_no(old_leaf_page_no);
        if !leaf_page.is_empty() {
            new_leaf_page_no = free_page_tracker.get_free_page(page_cache);
            leaf_page.set_page_number(new_leaf_page_no);
            leaf_page.set_version(new_version);
            page_cache.put_page(leaf_page.get_page());
        }

        // Need to walk back up stack, fix_stack will return the new root page number. We have
        // delete the key so return true.
        let new_root_page_no = TreeDeleteHandler::fix_stack(
            key,
            &mut dir_pages,
            free_page_tracker,
            page_cache,
            new_version,
            new_leaf_page_no,
            old_leaf_page_no,
        );
        return (new_root_page_no, true);
    }

    fn fix_stack(
        key: &Vec<u8>,
        dir_pages: &mut Vec<TreeDirPage>,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
        new_leaf_page_no: u64,
        old_leaf_page_no: u64,
    ) -> u64 {
        // if new_leaf_page_no is not 0 then we just need to rewrite the dir pages, none of them
        // the leaf page still exists and we do not rebalance.
        if new_leaf_page_no != 0 {
            return TreeDeleteHandler::fix_stack_no_page_del(
                key,
                dir_pages,
                free_page_tracker,
                page_cache,
                new_version,
                new_leaf_page_no,
            );
        }

        // Need to handle page deletion
        return TreeDeleteHandler::fix_stack_page_del(
            key,
            dir_pages,
            free_page_tracker,
            page_cache,
            new_version,
            old_leaf_page_no,
        );
    }

    fn fix_stack_page_del(
        key: &Vec<u8>,
        dir_pages: &mut Vec<TreeDirPage>,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
        old_leaf_page_no: u64,
    ) -> u64 {
        let mut page_to_delete = old_leaf_page_no;
        loop {
            let dir_page_wrapped = dir_pages.pop();
            if dir_page_wrapped.is_none() {
                // we have removed the root of the tree
                break;
            }
            let mut dir_page = dir_page_wrapped.unwrap();
            dir_page.remove_key_page(key, page_to_delete);
            if dir_page.is_empty() {
                page_to_delete = dir_page.get_page_number();
                free_page_tracker.return_free_page_no(page_to_delete);
            } else {
                // This dir page is not empty - push back on stack for
                // remapping.
                dir_pages.push(dir_page);
                break;
            }
        }

        // We have nuked the root of the tree - need to create a TreeLeaf to replace it.
        if dir_pages.is_empty() {
            let new_root_page_no = free_page_tracker.get_free_page(page_cache);
            let mut new_root =
                TreeLeafPage::create_new(page_cache.get_page_config(), new_root_page_no);
            new_root.set_version(new_version);
            page_cache.put_page(new_root.get_page());
            return new_root_page_no;
        }

        // There are a stack of dir pages to rewrite
        return TreeDeleteHandler::fix_dir_stack(
            key,
            dir_pages,
            free_page_tracker,
            page_cache,
            new_version,
        );
    }

    fn fix_dir_stack(
        key: &Vec<u8>,
        dir_pages: &mut Vec<TreeDirPage>,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
    ) -> u64 {
        let mut new_page_no = 0;
        loop {
            // Note there has to be at least one
            let dir_page_wrapped = dir_pages.pop();
            if dir_page_wrapped.is_none() {
                break;
            }
            let mut dir_page = dir_page_wrapped.unwrap();
            let old_page_no = dir_page.get_page_number();
            free_page_tracker.return_free_page_no(old_page_no);
            // The first dir_page we pop does not need its directory entries changed.
            if new_page_no != 0 {
                let tree_dir_entry = TreeDirEntry::new(key.clone(), new_page_no);
                let mut entries: Vec<TreeDirEntry> = Vec::new();
                entries.push(tree_dir_entry);
                dir_page.add_entries(entries);
            }
            new_page_no = free_page_tracker.get_free_page(page_cache);
            dir_page.set_page_number(new_page_no);
            dir_page.set_version(new_version);
            page_cache.put_page(dir_page.get_page());
        }

        return new_page_no;
    }

    fn fix_stack_no_page_del(
        key: &Vec<u8>,
        dir_pages: &mut Vec<TreeDirPage>,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
        new_leaf_page_no: u64,
    ) -> u64 {
        let mut page_no_to_update = new_leaf_page_no;
        loop {
            let dir_page_wrapped = dir_pages.pop();
            if dir_page_wrapped.is_none() {
                break;
            }
            let mut dir_page = dir_page_wrapped.unwrap();
            // Update the entry in the dir page with the new page number
            let tree_dir_entry = TreeDirEntry::new(key.clone(), page_no_to_update);
            let mut entries: Vec<TreeDirEntry> = Vec::new();
            entries.push(tree_dir_entry);
            dir_page.add_entries(entries);
            let dir_old_page_no = dir_page.get_page_number();
            free_page_tracker.return_free_page_no(dir_old_page_no);
            page_no_to_update = free_page_tracker.get_free_page(page_cache);
            dir_page.set_page_number(page_no_to_update);
            dir_page.set_version(new_version);
            page_cache.put_page(dir_page.get_page());
        }
        return page_no_to_update;
    }

    fn delete_key_from_root(
        key: &Vec<u8>,
        root_page: &mut TreeLeafPage,
        page_cache: &mut PageCache,
        free_page_tracker: &mut FreePageTracker,
        new_version: u64,
    ) -> (u64, bool) {
        let root_page_no = root_page.get_page_number();

        let tuple = root_page.delete_key(key);
        if tuple.is_none() {
            return (root_page_no, false);
        }

        // Store the root page back into the page cache.
        free_page_tracker.return_free_page_no(root_page_no);
        let new_root_page_no = free_page_tracker.get_free_page(page_cache);
        root_page.set_page_number(new_root_page_no);
        root_page.set_version(new_version);
        page_cache.put_page(root_page.get_page());

        let tuple_unwrapped = tuple.unwrap();
        if tuple_unwrapped.get_overflow() == Overflow::None {
            return (new_root_page_no, true);
        }

        // Overflow page - need to delete overflows.
        OverflowPageHandler::delete_overflow_tuple_pages(
            Some(tuple_unwrapped),
            page_cache,
            free_page_tracker,
        );

        return (new_root_page_no, true);
    }
}
