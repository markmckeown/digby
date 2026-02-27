use crate::OverflowPageHandler;
use crate::free_page_tracker::FreePageTracker;
use crate::leaf_page_handler::LeafPageHandler;
use crate::page::{Page, PageTrait, PageType};
use crate::page_cache::PageCache;
use crate::tree_dir_entry::TreeDirEntry;
use crate::tree_dir_handler::{TreeDirHandler, TreeDirPageRef};
use crate::tree_dir_page::TreeDirPage;
use crate::tree_leaf_page::TreeLeafPage;
use crate::tuple::{Tuple, TupleTrait};

pub struct StoreTupleProcessor {}

impl StoreTupleProcessor {
    pub fn get_tuple(key: &Vec<u8>, first: Page, page_cache: &mut PageCache) -> Option<Tuple> {
        // Set the page to be the first page, the root page.
        let mut page = first;

        loop {
            // If the page is a tree leaf then if the key is stored
            // then it will be in this leaf page.
            if page.get_type() == PageType::TreeLeaf {
                let tree_leaf = TreeLeafPage::from_page(page);
                return tree_leaf.get_tuple(key);
            }
            // If its a tree dir page then descend to the next
            // level.
            let dir_page = TreeDirPage::from_page(page);
            page = page_cache.get_page(dir_page.get_next_page(&key))
        }
    }

    // Given the root page of the tree store the tuple, the root page
    // could be a leaf page if the tree is empty or it could be dir
    // page.
    pub fn store_tuple(
        tuple: Tuple,
        first: Page,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
    ) -> u64 {
        if first.get_type() == PageType::TreeLeaf {
            // The root of the tree is actually a leaf page - requires special handling.
            let tree_root_single = TreeLeafPage::from_page(first);
            return StoreTupleProcessor::store_tuple_tree_root_single(
                tuple,
                tree_root_single,
                free_page_tracker,
                page_cache,
                new_version,
            );
        }

        // The root page is a tree dir page.
        let root_dir_page = TreeDirPage::from_page(first);
        return StoreTupleProcessor::store_tuple_tree(
            tuple,
            root_dir_page,
            free_page_tracker,
            page_cache,
            new_version,
        );
    }

    // The root page of the tree is a tree dir then descend into the tree
    // until find find the leaf page, then add the tuple.
    // As descend the tree keep track of the tree dir so they can be updated
    // after the tuple is added.
    fn store_tuple_tree(
        tuple: Tuple,
        root_dir_page: TreeDirPage,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
    ) -> u64 {
        let mut dir_page = root_dir_page;
        // This is the stack for storing the tree dir as we descend into
        // the tree.
        let mut dir_pages: Vec<TreeDirPage> = Vec::new();
        let mut next_page: u64;
        let leaf_page: TreeLeafPage;
        let key = tuple.get_key().to_vec();
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

        // Now have a leaf_page and a stack of dir pages.
        // Add to leaf page, remap leaf page or leaf pages if it split.
        // A leaf page can split into three depending on the size of tuples it holds.
        let update_result =
            LeafPageHandler::add_tuple(leaf_page, tuple, page_cache.get_page_config());

        // Clean up any overflow pages that may bave now be dangling if a tuple was overwritten
        OverflowPageHandler::delete_overflow_tuple_pages(
            update_result.deleted_tuple,
            page_cache,
            free_page_tracker,
        );

        // Remap leaf page or pages if it split and write to disk - get a set of dir entries back for the leadf pages.
        let leaf_dir_entries = StoreTupleProcessor::write_leaf_pages(
            update_result.tree_leaf_pages,
            free_page_tracker,
            page_cache,
            new_version,
        );

        // Add the leaf pages to the tree_dir_page on the top of the stack.
        dir_page = dir_pages.pop().unwrap();
        // Get a set of TreeDirEntryRefs back when updating the tree dir entry with the lead page details.
        // There could be more than one TreeDirEntryRef if the dir page had to split.
        let mut dir_refs = TreeDirHandler::handle_tree_leaf_store(
            dir_page,
            leaf_dir_entries,
            page_cache.get_page_config(),
        );
        // Write the dir entries out to disk and get back a set of directory entries back.
        let mut dir_entries = StoreTupleProcessor::write_tree_dir_pages(
            dir_refs,
            free_page_tracker,
            page_cache,
            new_version,
        );

        // Need to walk back up the directory stack adding the pages.
        while !dir_pages.is_empty() {
            dir_page = dir_pages.pop().unwrap();
            dir_refs = TreeDirHandler::handle_tree_dir_store(
                dir_page,
                dir_entries,
                new_version,
                page_cache.get_page_config(),
            );
            dir_entries = StoreTupleProcessor::write_tree_dir_pages(
                dir_refs,
                free_page_tracker,
                page_cache,
                new_version,
            );
        }

        // If after walking the stack there is only one dir_entry then the root has not split - we can just return its page number.
        if dir_entries.len() == 1 {
            return dir_entries.get(0).unwrap().get_page_no();
        }

        // We have hit the top of the stack but have two dir entries, the root has split.
        // Need to create a new root, register the entries and return the reference to the root.
        // Need a new TreeDirPage.
        let new_tree_dir_page = TreeDirPage::create_new(page_cache.get_page_config(), 0, 0);
        // Add the entries to the new root page.
        dir_refs = TreeDirHandler::handle_tree_dir_store(
            new_tree_dir_page,
            dir_entries,
            new_version,
            page_cache.get_page_config(),
        );
        // The new root page cannot split - so there should only be one page in the dir_refs now.
        dir_entries = StoreTupleProcessor::write_tree_dir_pages(
            dir_refs,
            free_page_tracker,
            page_cache,
            new_version,
        );
        assert!(dir_entries.len() == 1);
        return dir_entries.get(0).unwrap().get_page_no();
    }

    // Write out the dir pages, we are passed TreeDirPageRef. When splitting
    // dir pages we need to be careful about knowing what the left most key is
    // for the page - note this is not explicitly stored in the page.
    fn write_tree_dir_pages(
        mut dir_pages: Vec<TreeDirPageRef>,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
    ) -> Vec<TreeDirEntry> {
        // Change the page numbers to free pages and return the old page numbers to
        // be recycled in future commits.
        TreeDirHandler::map_pages(&mut dir_pages, free_page_tracker, page_cache, new_version);
        // We want to generate a set of tree dir entries
        let mut entries: Vec<TreeDirEntry> = Vec::new();
        for mut dir_page in dir_pages {
            let tree_dir_entry: TreeDirEntry;
            if dir_page.left_key.is_none() {
                // If key is none then this was the old page that was split.
                tree_dir_entry = TreeDirEntry::new(
                    dir_page.page.get_dir_left_key().unwrap(),
                    dir_page.page.get_page_number(),
                );
            } else {
                // This is a new dir page that came from a split.
                tree_dir_entry =
                    TreeDirEntry::new(dir_page.left_key.unwrap(), dir_page.page.get_page_number());
            }
            entries.push(tree_dir_entry);
            // Write the page to disk.
            page_cache.put_page(dir_page.page.get_page());
        }
        return entries;
    }

    // Get new page numbers for the leaf pages and return the old page numbers
    // to be reused in future commits.
    fn write_leaf_pages(
        mut leaf_pages: Vec<TreeLeafPage>,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
    ) -> Vec<TreeDirEntry> {
        LeafPageHandler::map_pages(&mut leaf_pages, free_page_tracker, page_cache, new_version);
        // We return a set do dir entries for the next phase.
        let mut entries: Vec<TreeDirEntry> = Vec::new();
        for mut leaf_page in leaf_pages {
            // Create a TreeDirEntry for the leaf page to add to the TreeDirPage
            let tree_dir_entry = TreeDirEntry::new(
                leaf_page.get_left_key().unwrap(),
                leaf_page.get_page_number(),
            );
            entries.push(tree_dir_entry);
            // Write the leaf page to disk, after the map_pages call above this will write the page over a free page.
            page_cache.put_page(leaf_page.get_page());
        }
        return entries;
    }

    // The root page of the tree is a leaf page - this means either:
    //  - the tuple can be added to the page.
    //  - the page needs to be split and a new root page created that is
    //    a dir page.
    fn store_tuple_tree_root_single(
        tuple: Tuple,
        tree_root_single: TreeLeafPage,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
    ) -> u64 {
        // Add the tuple to the leaf page.
        let mut update_result =
            LeafPageHandler::add_tuple(tree_root_single, tuple, page_cache.get_page_config());

        // Clean up any overflow pages that may bave now be dangling if a tuple was overwritten
        OverflowPageHandler::delete_overflow_tuple_pages(
            update_result.deleted_tuple,
            page_cache,
            free_page_tracker,
        );

        // Update the leaf page numbers so they are write over free pages and also set the version.
        LeafPageHandler::map_pages(
            &mut update_result.tree_leaf_pages,
            free_page_tracker,
            page_cache,
            new_version,
        );
        if update_result.tree_leaf_pages.len() == 1 {
            // The root leaf page has not split - grab the new page number for the root leaf page.
            let page_number = update_result
                .tree_leaf_pages
                .get(0)
                .unwrap()
                .get_page_number();
            // Write the new root leaf page to disk
            page_cache.put_page(update_result.tree_leaf_pages.pop().unwrap().get_page());
            // Return the new root page_number
            return page_number;
        }

        // The root leaf page has split. Need a new TreeDirPage that will act as the root and hold the
        // the new leaf pages. There could be up to three leaf pages if the entries are large.
        let mut entries: Vec<TreeDirEntry> = Vec::new();
        for mut leaf_page in update_result.tree_leaf_pages {
            // Create a TreeDirEntry for the leaf page to add to the TreeDirPage
            let tree_dir_entry = TreeDirEntry::new(
                leaf_page.get_left_key().unwrap(),
                leaf_page.get_page_number(),
            );
            entries.push(tree_dir_entry);
            // Write the leaf page to disk, after the map_pages call above this will write the page over a free page.
            page_cache.put_page(leaf_page.get_page());
        }
        // Need a new TreeDirPage.
        let new_tree_dir_page = TreeDirPage::create_new(page_cache.get_page_config(), 0, 0);
        // Add the entries to the new root page.
        let dir_refs = TreeDirHandler::handle_tree_leaf_store(
            new_tree_dir_page,
            entries,
            page_cache.get_page_config(),
        );
        // The new root page cannot split - there can be a most three entries added to it.
        assert!(dir_refs.len() == 1);
        let dir_entries = StoreTupleProcessor::write_tree_dir_pages(
            dir_refs,
            free_page_tracker,
            page_cache,
            new_version,
        );
        assert!(dir_entries.len() == 1);
        return dir_entries.get(0).unwrap().get_page_no();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_is_leaf_add_1() {
        let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
        let version = 23;
        let db_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&temp_file)
            .expect("Failed to open or create DB file");

        let file_layer: crate::FileLayer =
            crate::FileLayer::new(db_file, crate::Db::BLOCK_SIZE as usize);
        let block_layer: crate::BlockLayer =
            crate::BlockLayer::new(file_layer, crate::Db::BLOCK_SIZE as usize);
        let mut page_cache: crate::PageCache = crate::PageCache::new(block_layer);

        let free_dir_page_no = *page_cache.generate_free_pages(1).get(0).unwrap();
        let mut free_dir_page =
            crate::FreeDirPage::create_new(page_cache.get_page_config(), free_dir_page_no, version);
        page_cache.put_page(free_dir_page.get_page());

        let root_tree_page_no = *page_cache.generate_free_pages(1).get(0).unwrap();
        let mut leaf_page =
            TreeLeafPage::create_new(page_cache.get_page_config(), root_tree_page_no);
        leaf_page.set_version(version);
        page_cache.put_page(leaf_page.get_page());

        let mut free_page_tracker = FreePageTracker::new(
            page_cache.get_page(free_dir_page_no),
            version + 1,
            *page_cache.get_page_config(),
        );

        let reloaded_page = page_cache.get_page(root_tree_page_no);

        let tuple = Tuple::new(
            b"key_1".to_vec().as_ref(),
            b"value_1".to_vec().as_ref(),
            version + 1,
        );
        let new_root_tree_no = StoreTupleProcessor::store_tuple(
            tuple,
            reloaded_page,
            &mut free_page_tracker,
            &mut page_cache,
            version + 1,
        );
        assert_eq!(new_root_tree_no, 2);

        std::fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

    #[test]
    fn test_root_is_leaf_add_multiple() {
        let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
        let mut version = 23;
        let db_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&temp_file)
            .expect("Failed to open or create DB file");

        let file_layer: crate::FileLayer =
            crate::FileLayer::new(db_file, crate::Db::BLOCK_SIZE as usize);
        let block_layer: crate::BlockLayer =
            crate::BlockLayer::new(file_layer, crate::Db::BLOCK_SIZE as usize);
        let mut page_cache: crate::PageCache = crate::PageCache::new(block_layer);

        let mut free_dir_page_no = *page_cache.generate_free_pages(1).get(0).unwrap();
        let mut free_dir_page =
            crate::FreeDirPage::create_new(page_cache.get_page_config(), free_dir_page_no, version);
        page_cache.put_page(free_dir_page.get_page());

        let mut root_tree_page_no = *page_cache.generate_free_pages(1).get(0).unwrap();
        let mut leaf_page =
            TreeLeafPage::create_new(page_cache.get_page_config(), root_tree_page_no);
        leaf_page.set_version(version);
        page_cache.put_page(leaf_page.get_page());

        let mut j: u32 = 0;
        for i in 0u32..4000 {
            j = i;
            version = version + 1;
            let mut free_page_tracker = FreePageTracker::new(
                page_cache.get_page(free_dir_page_no),
                version,
                *page_cache.get_page_config(),
            );
            let reloaded_page = page_cache.get_page(root_tree_page_no);
            let tuple = Tuple::new(
                i.to_be_bytes().to_vec().as_ref(),
                i.to_be_bytes().to_vec().as_ref(),
                version,
            );
            root_tree_page_no = StoreTupleProcessor::store_tuple(
                tuple,
                reloaded_page,
                &mut free_page_tracker,
                &mut page_cache,
                version + 1,
            );
            let free_pages = free_page_tracker.get_free_dir_pages(&mut page_cache);
            free_dir_page_no = free_pages.last().unwrap().get_page_number();
            for mut free_page in free_pages {
                page_cache.put_page(free_page.get_page());
            }
            if page_cache.get_page(root_tree_page_no).get_type() != PageType::TreeLeaf {
                break;
            }
        }
        assert_eq!(j, 193);

        let root_page = TreeDirPage::from_page(page_cache.get_page(root_tree_page_no));
        // There are two leaf pages, but only 1 key stored.
        assert_eq!(root_page.get_entries(), 1);
        std::fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

    #[test]
    fn test_add_multiple_pages() {
        let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
        let mut version = 23;
        let db_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&temp_file)
            .expect("Failed to open or create DB file");

        let file_layer: crate::FileLayer =
            crate::FileLayer::new(db_file, crate::Db::BLOCK_SIZE as usize);
        let block_layer: crate::BlockLayer =
            crate::BlockLayer::new(file_layer, crate::Db::BLOCK_SIZE as usize);
        let mut page_cache: crate::PageCache = crate::PageCache::new(block_layer);

        let mut free_dir_page_no = *page_cache.generate_free_pages(1).get(0).unwrap();
        let mut free_dir_page =
            crate::FreeDirPage::create_new(page_cache.get_page_config(), free_dir_page_no, version);
        page_cache.put_page(free_dir_page.get_page());

        let mut root_tree_page_no = *page_cache.generate_free_pages(1).get(0).unwrap();
        let mut leaf_page =
            TreeLeafPage::create_new(page_cache.get_page_config(), root_tree_page_no);
        leaf_page.set_version(version);
        page_cache.put_page(leaf_page.get_page());

        for i in 0u64..20000 {
            version = version + 1;
            let mut free_page_tracker = FreePageTracker::new(
                page_cache.get_page(free_dir_page_no),
                version,
                *page_cache.get_page_config(),
            );
            let reloaded_page = page_cache.get_page(root_tree_page_no);
            let tuple = Tuple::new(
                i.to_be_bytes().to_vec().as_ref(),
                i.to_be_bytes().to_vec().as_ref(),
                version,
            );
            root_tree_page_no = StoreTupleProcessor::store_tuple(
                tuple,
                reloaded_page,
                &mut free_page_tracker,
                &mut page_cache,
                version + 1,
            );
            let free_pages = free_page_tracker.get_free_dir_pages(&mut page_cache);
            free_dir_page_no = free_pages.last().unwrap().get_page_number();
            for mut free_page in free_pages {
                page_cache.put_page(free_page.get_page());
            }
            page_cache.sync_data();
        }

        let root_page = page_cache.get_page(root_tree_page_no);
        assert!(root_page.get_type() == PageType::TreeDirPage);
        let root_dir_page = TreeDirPage::from_page(page_cache.get_page(root_tree_page_no));
        // There should be 42 entries.
        assert_eq!(root_dir_page.get_entries(), 1);
        let tuple = StoreTupleProcessor::get_tuple(
            13000u64.to_be_bytes().to_vec().as_ref(),
            root_page,
            &mut page_cache,
        );
        assert!(!tuple.is_none());
        assert!(tuple.unwrap().get_value() == 13000u64.to_be_bytes().to_vec());
        std::fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }
}
