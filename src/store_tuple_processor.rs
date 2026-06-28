use crate::OverflowPageHandler;
use crate::PageNo;
use crate::dir_page::DirPage;
use crate::free_page_tracker::FreePageTracker;
use crate::leaf_page::LeafPage;
use crate::leaf_page_handler::LeafPageHandler;
use crate::page::{Page, PageTrait, PageType};
use crate::page_cache::PageCache;
use crate::tree_dir_entry::TreeDirEntry;
use crate::tree_dir_handler::{DirPageRef, TreeDirHandler};
use crate::tuple::{Tuple, TupleTrait};

pub struct StoreTupleProcessor {}

impl StoreTupleProcessor {
    // Get a tuple.
    // The root page of the tree is supplied, this could be a leaf page
    // or a directory page.
    // We use get_page_ref to access the page cache, this supplies shared
    // references to the page rather than copies of the pages as when
    // used with update and delete.
    pub fn get_tuple(key: &[u8], page_no: u64, page_cache: &mut PageCache) -> Option<Tuple> {
        let mut page_number = page_no;
        loop {
            let page = page_cache.get_page_ref(PageNo::from_u64(page_number));
            // If the page is a tree leaf then if the key is stored
            // then it will be in this leaf page.
            if page.get_type() == PageType::LeafPage {
                return LeafPage::get_tuple_from_page(page, key);
            }
            // If its a tree dir page then descend to the next
            // level.
            page_number = DirPage::get_next_page(page, key);
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
        // Special case if the first page is a leaf page.
        if first.get_type() == PageType::LeafPage {
            // The root of the tree is actually a leaf page - requires special handling.
            let tree_root_single = LeafPage::from_page(first);
            return StoreTupleProcessor::store_tuple_tree_root_single(
                tuple,
                tree_root_single,
                free_page_tracker,
                page_cache,
                new_version,
            );
        }

        // The root page is a tree dir page, convert to dir page
        // and descend into the tree to find the correct leaf page
        // to add the tuple too.
        let root_dir_page = DirPage::from_page(first);
        StoreTupleProcessor::store_tuple_tree(
            tuple,
            root_dir_page,
            free_page_tracker,
            page_cache,
            new_version,
        )
    }

    // The root page of the tree is a tree dir then descend into the tree
    // until we find the leaf page, then add the tuple.
    // As descend the tree keep track of the tree dir so they can be updated
    // after the tuple is added. The directory pages are stored on a stack.
    //
    // Returns the page number of the root page of the tree after adding
    // the tuple.
    fn store_tuple_tree(
        tuple: Tuple,
        root_dir_page: DirPage,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
    ) -> u64 {
        let mut dir_page = root_dir_page;
        // This is the stack for storing the tree dir as we descend into
        // the tree.
        let mut dir_pages: Vec<DirPage> = Vec::new();
        let mut next_page_no: u64;
        let leaf_page: LeafPage;
        let key = tuple.get_key();

        // loop down until we hit the leaf page keeping a track of the
        // the dir pages as we go.
        loop {
            // Get the next page number of the next page from the
            // directory node
            next_page_no = dir_page.get_next(key);
            // Push the directory node onto the stack to update later.
            dir_pages.push(dir_page);
            // Get the page from the cache - this is copy of the page.
            let page = page_cache.get_page(PageNo::from_u64(next_page_no));
            // If the page is a leaf page we can start the add process
            if page.get_type() == PageType::LeafPage {
                leaf_page = LeafPage::from_page(page);
                break;
            }
            dir_page = DirPage::from_page(page);
        }

        // Now have a leaf_page and a stack of dir pages.
        // Add to leaf page, remap leaf page or leaf pages if it split.
        // A leaf page can split into three depending on the size of tuples it holds.
        let update_result = LeafPageHandler::add_tuple(leaf_page, tuple);

        // Clean up any overflow pages that may bave now be dangling if a tuple
        // was overwritten. The method add_tuple will return any tuple that
        // was overwritten in the update_result, this tuple could point to an
        // overflow tuple - in that case the overflow tuple needs to be deleted.
        OverflowPageHandler::delete_overflow_tuple_pages(
            update_result.deleted_tuple,
            page_cache,
            free_page_tracker,
        );

        // Remap leaf page, or pages if it split and write to disk - get a set of
        // dir entries back for the leaf pages. These dir entries are used to update
        // the leaf pages parent node.
        let leaf_dir_entries = StoreTupleProcessor::write_leaf_pages(
            update_result.tree_leaf_pages,
            free_page_tracker,
            page_cache,
            new_version,
        );

        // Get the parent dir page for the leaf pages, this is the page at the
        // top of the stack. There will be at least one dir page.
        dir_page = dir_pages.pop().unwrap();

        // Store the leaf page references into the dir page.
        // The leaf page may have split, when adding the split leaf pages
        // into the dir page it may need to split also - handle_tree_leaf_stor
        // handles this and returns a set of TreeDirEntryRefs that are added
        // to the parent node.
        // There could be more than one TreeDirEntryRef if the dir page had to split.
        let mut dir_refs = TreeDirHandler::handle_tree_leaf_store(dir_page, leaf_dir_entries);
        // Write the dir entries out to disk and get back a set of directory entries back
        // - for the parent node of the lead node this will mean returning its old
        // page number and getting a new one; for new split dir pages they will get
        // new page numbers.
        let mut dir_entries = StoreTupleProcessor::write_tree_dir_pages(
            dir_refs,
            free_page_tracker,
            page_cache,
            new_version,
        );

        // Need to walk back up the directory stack adding the pages.
        // The steps in the loop are similar to the three steps above,
        // the difference is that handle_tree_dir_store is used instead
        // of handle_tree_leaf_store as we are dealing with dir
        // pages now.
        while !dir_pages.is_empty() {
            dir_page = dir_pages.pop().unwrap();
            dir_refs = TreeDirHandler::handle_tree_dir_store(dir_page, dir_entries);
            dir_entries = StoreTupleProcessor::write_tree_dir_pages(
                dir_refs,
                free_page_tracker,
                page_cache,
                new_version,
            );
        }

        // If after walking the stack there is only one dir_entry
        // then the root has not split - we can just return its page number.
        if dir_entries.len() == 1 {
            return dir_entries.first().unwrap().get_page_no();
        }

        // We have hit the top of the stack but have two dir entries, the root has split.
        // Need to create a new root, register the entries and return the reference to the root.
        // Need a new TreeDirPage.
        let new_tree_dir_page =
            DirPage::create_new(page_cache.get_page_config(), PageNo::from_u64(0), 0);
        // Add the entries to the new root page.
        dir_refs = TreeDirHandler::handle_tree_dir_store(new_tree_dir_page, dir_entries);
        // The new root page cannot split - so there should only be one page in the dir_refs now.
        dir_entries = StoreTupleProcessor::write_tree_dir_pages(
            dir_refs,
            free_page_tracker,
            page_cache,
            new_version,
        );
        assert!(dir_entries.len() == 1);
        dir_entries.first().unwrap().get_page_no()
    }

    // Write out the dir pages, we are passed TreeDirPageRef. When splitting
    // dir pages we need to be careful about knowing what the left most key is
    // for the page - note this is not explicitly stored in the page.
    fn write_tree_dir_pages(
        mut dir_pages: Vec<DirPageRef>,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
    ) -> Vec<TreeDirEntry> {
        // Change the page numbers to free pages and return the old page numbers to
        // be recycled in future commits.
        TreeDirHandler::map_dir_pages(&mut dir_pages, free_page_tracker, page_cache, new_version);
        // We want to generate a set of tree dir entries
        let mut entries: Vec<TreeDirEntry> = Vec::new();
        for mut dir_page in dir_pages {
            // if dir_page.left_key is None then this is the original page from
            // a dir page split, we need to retrieve the left key for this page.
            let left_key = dir_page
                .left_key
                .or_else(|| dir_page.page.get_dir_left_key())
                .unwrap();

            let tree_dir_entry =
                TreeDirEntry::new(left_key, dir_page.page.get_page_number().to_u64());
            entries.push(tree_dir_entry);

            // Write the page to disk.
            page_cache.put_page(dir_page.page.get_page());
        }
        entries
    }

    // Write out a set of leaf pages.
    // After adding a tuple to a leaf page then write out the leaf
    // page. If the page split there may be more than one leaf page.
    // Before writing out the pages we need to get new page numbers
    // for the pages and set their versions.
    fn write_leaf_pages(
        mut leaf_pages: Vec<(LeafPage, Option<Vec<u8>>)>,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
    ) -> Vec<TreeDirEntry> {
        // Get new page numbers for the leaf pages and set the version. For the
        // the original page its version number will be returned to the
        // free page tracker.
        LeafPageHandler::map_pages(&mut leaf_pages, free_page_tracker, page_cache, new_version);
        // We return a set dir entries for the next phase, these are used to update
        // the parent directory node for the pages.
        let mut entries: Vec<TreeDirEntry> = Vec::new();
        for (mut leaf_page, left_key_for_page) in leaf_pages {
            // Create a TreeDirEntry for the leaf page to add to the DirPage.
            // This is made up of the left most key in the page and the page number of the page.
            let key =
                left_key_for_page.unwrap_or_else(|| leaf_page.get_left_key().unwrap().to_vec());
            let tree_dir_entry = TreeDirEntry::new(key, leaf_page.get_page_number().to_u64());
            entries.push(tree_dir_entry);
            // Write the leaf page to disk, after the map_pages call above this
            // will write the page over a free page.
            page_cache.put_page(leaf_page.get_page());
        }
        entries
    }

    // The root page of the tree is a leaf page - this means either:
    //  - the tuple can be added to the page.
    //  - the page needs to be split and a new root page created that is a dir page.
    fn store_tuple_tree_root_single(
        tuple: Tuple,
        tree_root_single: LeafPage,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
    ) -> u64 {
        // Add the tuple to the leaf page.
        let mut update_result = LeafPageHandler::add_tuple(tree_root_single, tuple);

        // Clean up any overflow pages that may now be dangling if a tuple was overwritten
        OverflowPageHandler::delete_overflow_tuple_pages(
            update_result.deleted_tuple,
            page_cache,
            free_page_tracker,
        );

        // Update the leaf page numbers so they write over free pages and also set the version.
        LeafPageHandler::map_pages(
            &mut update_result.tree_leaf_pages,
            free_page_tracker,
            page_cache,
            new_version,
        );

        if update_result.tree_leaf_pages.len() == 1 {
            // The root leaf page has not split - grab the new page number for the root leaf page.
            let (mut root_leaf_page, _) = update_result.tree_leaf_pages.pop().unwrap();
            let page_number = root_leaf_page.get_page_number().to_u64();
            // Write the new root leaf page to disk
            page_cache.put_page(root_leaf_page.get_page());
            // Return the new root page_number
            return page_number;
        }

        // The root leaf page has split. Need a new DirPage that will act as the root and hold the
        // the new leaf pages. There could be up to three leaf pages if the entries are large.
        let mut entries: Vec<TreeDirEntry> = Vec::new();
        for (mut leaf_page, page_left_key) in update_result.tree_leaf_pages {
            // Create a TreeDirEntry for the leaf page to add to the TreeDirPage
            let key = page_left_key.unwrap_or_else(|| leaf_page.get_left_key().unwrap().to_vec());
            let tree_dir_entry = TreeDirEntry::new(key, leaf_page.get_page_number().to_u64());
            entries.push(tree_dir_entry);
            // Write the leaf page to disk, after the map_pages call above this
            // will write the page over a free page.
            page_cache.put_page(leaf_page.get_page());
        }
        // Need a new DirPage.
        let new_tree_dir_page =
            DirPage::create_new(page_cache.get_page_config(), PageNo::from_u64(0), 0);
        // Add the entries to the new root page.
        let dir_refs = TreeDirHandler::handle_tree_leaf_store(new_tree_dir_page, entries);
        // The new root page cannot split - there can be a most three entries added to it.
        assert!(dir_refs.len() == 1);
        let dir_entries = StoreTupleProcessor::write_tree_dir_pages(
            dir_refs,
            free_page_tracker,
            page_cache,
            new_version,
        );
        assert!(dir_entries.len() == 1);
        dir_entries.first().unwrap().get_page_no()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_no;

    #[test]
    fn test_root_is_leaf_add_1() {
        let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
        let version = 23;
        let db_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(&temp_file)
            .expect("Failed to open or create DB file");

        let file_layer: crate::FileLayer = crate::FileLayer::new(db_file, crate::Db::BLOCK_SIZE);
        let block_layer: crate::PageContainerLayer =
            crate::PageContainerLayer::new(file_layer, crate::Db::BLOCK_SIZE);
        let mut page_cache: crate::PageCache = crate::PageCache::new(block_layer);

        let free_dir_page_no = *page_cache.generate_free_pages(1, 0).first().unwrap();
        let mut free_dir_page =
            crate::FreeDirPage::create_new(page_cache.get_page_config(), free_dir_page_no, version);
        page_cache.put_page(free_dir_page.get_page());

        let root_tree_page_no = *page_cache.generate_free_pages(1, 0).first().unwrap();
        let mut leaf_page =
            LeafPage::create_new(page_cache.get_page_config(), root_tree_page_no, version);
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
            .truncate(true)
            .open(&temp_file)
            .expect("Failed to open or create DB file");

        let file_layer: crate::FileLayer = crate::FileLayer::new(db_file, crate::Db::BLOCK_SIZE);
        let block_layer: crate::PageContainerLayer =
            crate::PageContainerLayer::new(file_layer, crate::Db::BLOCK_SIZE);
        let mut page_cache: crate::PageCache = crate::PageCache::new(block_layer);

        let mut free_dir_page_no = *page_cache.generate_free_pages(1, 0).first().unwrap();
        let mut free_dir_page =
            crate::FreeDirPage::create_new(page_cache.get_page_config(), free_dir_page_no, version);
        page_cache.put_page(free_dir_page.get_page());

        let mut root_tree_page_no = *page_cache.generate_free_pages(1, 0).first().unwrap();
        let mut leaf_page =
            LeafPage::create_new(page_cache.get_page_config(), root_tree_page_no, version);
        page_cache.put_page(leaf_page.get_page());

        let mut j: u32 = 0;
        for i in 0u32..4000 {
            j = i;
            version += 1;
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
            root_tree_page_no = page_no::PageNo(StoreTupleProcessor::store_tuple(
                tuple,
                reloaded_page,
                &mut free_page_tracker,
                &mut page_cache,
                version + 1,
            ));
            let free_pages = free_page_tracker.get_free_dir_pages(&mut page_cache);
            free_dir_page_no = free_pages.last().unwrap().get_page_number();
            for mut free_page in free_pages {
                page_cache.put_page(free_page.get_page());
            }
            if page_cache.get_page(root_tree_page_no).get_type() != PageType::LeafPage {
                break;
            }
        }
        assert_eq!(j, 193);

        let root_page = DirPage::from_page(page_cache.get_page(root_tree_page_no));
        // There are two leaf pages, but only 1 key stored.
        assert_eq!(root_page.get_entries_size(), 1);
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
            .truncate(true)
            .open(&temp_file)
            .expect("Failed to open or create DB file");

        let file_layer: crate::FileLayer = crate::FileLayer::new(db_file, crate::Db::BLOCK_SIZE);
        let block_layer: crate::PageContainerLayer =
            crate::PageContainerLayer::new(file_layer, crate::Db::BLOCK_SIZE);
        let mut page_cache: crate::PageCache = crate::PageCache::new(block_layer);

        let mut free_dir_page_no = *page_cache.generate_free_pages(1, 0).first().unwrap();
        let mut free_dir_page =
            crate::FreeDirPage::create_new(page_cache.get_page_config(), free_dir_page_no, version);
        page_cache.put_page(free_dir_page.get_page());

        let mut root_tree_page_no = *page_cache.generate_free_pages(1, 0).first().unwrap();
        let mut leaf_page =
            LeafPage::create_new(page_cache.get_page_config(), root_tree_page_no, version);
        page_cache.put_page(leaf_page.get_page());

        for i in 0u64..20000 {
            version += 1;
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
            root_tree_page_no = page_no::PageNo(StoreTupleProcessor::store_tuple(
                tuple,
                reloaded_page,
                &mut free_page_tracker,
                &mut page_cache,
                version + 1,
            ));
            let free_pages = free_page_tracker.get_free_dir_pages(&mut page_cache);
            free_dir_page_no = free_pages.last().unwrap().get_page_number();
            for mut free_page in free_pages {
                page_cache.put_page(free_page.get_page());
            }
            page_cache.sync_data();
        }

        let root_page = page_cache.get_page(root_tree_page_no);
        assert!(root_page.get_type() == PageType::DirPage);
        let root_dir_page = DirPage::from_page(page_cache.get_page(root_tree_page_no));
        // There should be 42 entries.
        assert_eq!(root_dir_page.get_entries_size(), 1);
        let tuple = StoreTupleProcessor::get_tuple(
            13000u64.to_be_bytes().to_vec().as_ref(),
            root_tree_page_no.to_u64(),
            &mut page_cache,
        );
        assert!(tuple.is_some());
        assert!(tuple.unwrap().get_value() == 13000u64.to_be_bytes().to_vec());
        std::fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }
}
