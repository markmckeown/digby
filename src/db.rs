use crate::block_layer::PageContainerLayer;
use crate::block_sanity::BlockSanity;
use crate::compressor::CompressorType;
use crate::db_master_page::DbMasterPage;
use crate::db_root_page::DbRootPage;
use crate::file_layer::FileLayer;
use crate::free_page_tracker::FreePageTracker;
use crate::overflow_tuple::OverflowTuple;
use crate::page::PageTrait;
use crate::page_cache::PageCache;
use crate::page_no::PageNo;
use crate::tuple::{Overflow, Tuple, TupleTrait};
use crate::tx_ctx::TxCtx;
use crate::{
    ClearHandler, Compressor, FreeDirPage, LeafPage, OverflowPageHandler, StoreTupleProcessor,
    TreeDeleteHandler, TupleProcessor,
};

// Layers in the Db are:
//   file layer - manipulate the file holding the db nodes.
//   block layer - interacts with file layer, manages blocks which holds pages.
//   page cache - provides DB pages and interacts with block layer. Client gets and puts
//                pages.
//
// Compressor to use when compressing large tuples.
pub struct Db {
    page_cache: PageCache,
    compressor: Compressor,
}

impl Db {
    // Default block size, the page size is a function of the block size
    // depending on what block checksum is used or if encryption is being
    // used. For example if using a 4 byte checksum and no encryption then
    // the page size will be BLOCK_SIZE - 4.
    // TODO - should support multiple block sizes at once to allow very
    // large pages for large tuples.
    pub const BLOCK_SIZE: usize = 4096;

    // Create a DB object.
    //   path - the path to the file to use. If the file does not exist then create it for
    //          a new database. If the file exists sanity check it.
    //   key - optional. If provided use the key to encrypt/decrypt the db blocks. Once used
    //         for a database then should be consistently used.
    //   compressor_type - the compressor to use for large tuples.
    pub fn new(path: &str, key: Option<Vec<u8>>, compressor_type: CompressorType) -> Self {
        Db::new_with_page_size(path, key, compressor_type, Db::BLOCK_SIZE)
    }

    // As "new" but allows a different block_size to be used.
    pub fn new_with_page_size(
        path: &str,
        key: Option<Vec<u8>>,
        compressor_type: CompressorType,
        block_size: usize,
    ) -> Self {
        use std::fs::OpenOptions;
        use std::path::Path;

        let mut is_new = false;

        // Might make sense to lock the file.
        // If file exists open to append, new database,
        // else treat as an existing database
        let db_file: std::fs::File;
        if Path::new(path).exists() {
            db_file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .expect("Failed to open existing DB file");
            if std::fs::metadata(path).unwrap().len() == 0 {
                // If file is empty treat as new database.
                is_new = true;
            }
        } else {
            // File does not exist, create.
            db_file = OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .truncate(true) // Not necessary as file does not exist but clippy wants it.
                .open(path)
                .expect("Failed to open or create DB file");
            is_new = true;
        }

        // Set up the file layer with the open file.
        let file_layer: FileLayer = FileLayer::new(db_file, block_size);
        // Create block layer - this will depend on if encrytion is being
        // used or just checksums. There is no encryption and checksum as
        // the Aes128Gcm has built in checksum support.
        // TODO -  checksum hardcoded to xxHash32 and encryption to
        // AES-128-GCM.
        // File layer is passed to block layer.
        let block_layer: PageContainerLayer;
        let sanity_type: BlockSanity;
        if let Some(k) = key {
            block_layer = PageContainerLayer::new_with_key(file_layer, block_size, k);
            sanity_type = BlockSanity::Aes128Gcm;
        } else {
            block_layer = PageContainerLayer::new(file_layer, block_size);
            sanity_type = BlockSanity::XxH32Checksum;
        }
        // Create page cache with the block layer.
        let page_cache: PageCache = PageCache::new(block_layer);

        let mut db = Db {
            page_cache,
            compressor: Compressor::new(compressor_type),
        };

        if is_new {
            // Need to populate the new database with some metadata pages
            // including the sanity_type (encryption or checksum).
            db.init_db_file(sanity_type)
                .expect("Failed to initialize DB file");
        } else {
            // The DB already exists, check it is sane.
            db.check_db_integrity().expect("DB integrity check failed");
        }
        db
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        let mut tx_ctx = self.new_transaction();
        let deleted = self.delete_txn(key, &mut tx_ctx);
        self.commit(&mut tx_ctx);
        deleted
    }

    // Delete a key from the DB, returns a bool to indicate if the key was deleted.
    // If false the key did not exist.
    fn delete_txn(&mut self, key: &[u8], tx_ctx: &mut TxCtx) -> bool {
        // If the key is very large then a short version with a SHA256
        // hash will to stored as a reference in the DB tree. Need
        // to create a key that will be used for the operations.
        let key_to_use: Vec<u8> = if TupleProcessor::is_oversized_key(key) {
            TupleProcessor::generate_short_key(key)
        } else {
            key.to_owned()
        };

        // Get the page number of the root of the tree.
        let tree_root_page_no = tx_ctx.global_root_page_no;
        // Get the actual root page.
        let root_page = self
            .page_cache
            .get_page(PageNo::from_u64(tree_root_page_no));
        // Now pass to the TreeDeleteHandler to do the delete.
        let (new_tree_root_page_no, deleted) = TreeDeleteHandler::delete_key(
            &key_to_use,
            root_page,
            &mut self.page_cache,
            &mut tx_ctx.free_page_tracker,
            tx_ctx.new_version,
        );
        if !deleted {
            // If nothing deleted then pages do not need to be rewritten.
            return false;
        }
        tx_ctx.global_root_page_no = new_tree_root_page_no;
        deleted
    }

    // Dirty read - get a value in a transaction context.
    pub fn get_txn(&mut self, key: &[u8], tx_ctx: &TxCtx) -> Option<Vec<u8>> {
        self.get_from_tree(key, tx_ctx.global_root_page_no)
    }

    // Get the value associated with key in the DB. If the key
    // is not in the DB then None will be returned.
    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let master_page = self.get_master_page();
        let tree_page_no = master_page.get_global_tree_root_page_no();
        self.get_from_tree(key, tree_page_no)
    }

    // Given the tree root page number get the value associated with
    // the key in the DB if there is one.
    //
    // The tree_page_no can be the root of the global tree or
    // the root page of a table tree.
    fn get_from_tree(&mut self, key: &[u8], tree_page_no: u64) -> Option<Vec<u8>> {
        // If the key is very large then a shorted version is stored in the tree
        // using the SHA256 of the key.
        if !TupleProcessor::is_oversized_key(key) {
            // Not oversized so look up key.
            if let Some(tuple) =
                StoreTupleProcessor::get_tuple(key, tree_page_no, &mut self.page_cache)
            {
                // Found tuple, but it may be an overflow tuple (ie it has
                // a small key but a large value). Need to get overflow tuple
                // from the overflow pages.
                if tuple.get_overflow() != Overflow::None {
                    return self.get_overflow_tuple_value(key, &tuple);
                }
                return Some(self.get_tuple_value(&tuple));
            } else {
                return None;
            }
        }

        // Oversized key - get short version
        let short_key = TupleProcessor::generate_short_key(key);
        // This tuple will have a page number as the value, the page will be an overflow page
        // that forms a linked list of pages that will hold the tuple.
        let tuple = StoreTupleProcessor::get_tuple(&short_key, tree_page_no, &mut self.page_cache);
        // Do not have this key.
        tuple.as_ref()?;
        // The tuple exists, we have a reference to the overflow tuple in the overflow pages
        // in the tuple so look it up with get_overflow_tuple_value
        self.get_overflow_tuple_value(key, &tuple.unwrap())
    }

    // A tuple has been found but its an overflow tuple and holds
    // a reference to where the real tuple is, this function
    // resolves the overflow tuple to get the real tuple.
    fn get_overflow_tuple_value(&mut self, key: &[u8], tuple: &Tuple) -> Option<Vec<u8>> {
        assert!(tuple.get_overflow() != Overflow::None);
        // Tuple exists, the value will be a page number for the overflow page.
        let overflow_page_no = u64::from_le_bytes(tuple.get_value()[0..8].try_into().unwrap());
        let overflow_tuple: OverflowTuple =
            OverflowPageHandler::get_overflow_tuple(overflow_page_no, &mut self.page_cache);
        // Confirm the key is the same - would require a SHA256 clash to fail
        assert_eq!(
            key,
            self.get_tuple_key(&overflow_tuple),
            "BUG: Supplied key does not match key in returned OverflowTuple"
        );
        Some(self.get_tuple_value(&overflow_tuple))
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        let mut tx_ctx = self.new_transaction();
        self.put_txn(key, value, &mut tx_ctx);
        self.commit(&mut tx_ctx);
    }

    // Store a key and value in the db.
    pub fn put_txn(&mut self, key: &[u8], value: &[u8], tx_ctx: &mut TxCtx) {
        // Create the tuple we want to add. This could be an overflow
        // tuple - if it is an overflow tuple this method will
        // store the key/value in the overflow pages and the tuple
        // returned will have a reference to the overflow pages.
        let tuple = TupleProcessor::generate_tuple(
            key,
            value,
            &mut self.page_cache,
            &mut tx_ctx.free_page_tracker,
            tx_ctx.new_version,
            &self.compressor,
        );

        // Now get the page number of the root of the global tree.
        let tree_root_page_no = tx_ctx.global_root_page_no;
        // Now get the root page of the tree.
        let page = self
            .page_cache
            .get_page(PageNo::from_u64(tree_root_page_no));
        // Store the tuple, this will return the page number of the
        // new root of the page.
        let new_tree_root_page_no = StoreTupleProcessor::store_tuple(
            tuple,
            page,
            &mut tx_ctx.free_page_tracker,
            &mut self.page_cache,
            tx_ctx.new_version,
        );
        tx_ctx.global_root_page_no = new_tree_root_page_no;
    }

    pub fn clear(&mut self) {
        let mut tx_ctx = self.new_transaction();
        self.clear_txn(&mut tx_ctx);
        self.commit(&mut tx_ctx);
    }

    // Remove all entries in the root tree.
    // Note disk space is not freed up - the file stays
    // the same after the clear.
    pub fn clear_txn(&mut self, tx_ctx: &mut TxCtx) {
        // Now get the page number of the root of the global tree.
        let tree_root_page_no = tx_ctx.global_root_page_no;
        // Get the root of the tree.
        let page = self
            .page_cache
            .get_page(PageNo::from_u64(tree_root_page_no));
        // Clear the tree, will return the new root of the tree which
        // will now be a leaf page.
        tx_ctx.global_root_page_no = ClearHandler::clear_tree(
            page,
            &mut tx_ctx.free_page_tracker,
            &mut self.page_cache,
            tx_ctx.new_version,
        );
    }

    pub fn new_transaction(&mut self) -> TxCtx {
        let master_page = self.get_master_page();
        let old_version = master_page.get_version();
        let new_version = old_version + 1;
        // Find the free page directory that has the free page numbers.
        let free_page_dir_page_no = master_page.get_free_page_dir_page_no();
        let free_page_tracker = FreePageTracker::new(
            self.page_cache
                .get_page(PageNo::from_u64(free_page_dir_page_no)),
            new_version,
            *self.page_cache.get_page_config(),
        );
        TxCtx::new(master_page, new_version, free_page_tracker)
    }

    // Create a new table in the DB. A table is another b+ tree in the
    // DB, the root page to the table tree can be found in another tree,
    // the table directory tree.
    pub fn create_table(&mut self, name: &[u8]) {
        let mut tx_ctx = self.new_transaction();
        self.create_table_txn(name, &mut tx_ctx);
        self.commit(&mut tx_ctx);
    }

    pub fn create_table_txn(&mut self, name: &[u8], tx_ctx: &mut TxCtx) {
        // Assert on the things that cannot be handled yet.
        assert!(
            name.len() < u8::MAX as usize,
            "Cannot handle table name larger than u8::MAX."
        );

        // TODO Test if the table exists before creating.

        // Need to create a root page for the new table tree, the first page
        // in the tree will be a leaf page.
        let new_table_root_page_no = tx_ctx.free_page_tracker.get_free_page(&mut self.page_cache);
        let mut new_table_root_page = LeafPage::create_new(
            self.page_cache.get_page_config(),
            new_table_root_page_no,
            tx_ctx.new_version,
        );
        // Store the new root page back into the file.
        self.page_cache.put_page(new_table_root_page.get_page());

        // Create the tuple that will be the reference to the new table
        // that will be stored in the table directory tree.
        // The tuple will have a key of the table name and value of the
        // root page number for the table's tree.
        let tuple = TupleProcessor::generate_tuple(
            name,
            &new_table_root_page_no.to_le_bytes(),
            &mut self.page_cache,
            &mut tx_ctx.free_page_tracker,
            tx_ctx.new_version,
            &self.compressor,
        );

        // Get the root page of the table directory tree.
        let table_tree_root_page_no = tx_ctx.tree_dir_root_page_no;
        let page = self
            .page_cache
            .get_page(PageNo::from_u64(table_tree_root_page_no));
        // Store the reference to the new table in the table
        // directory tree.
        tx_ctx.tree_dir_root_page_no = StoreTupleProcessor::store_tuple(
            tuple,
            page,
            &mut tx_ctx.free_page_tracker,
            &mut self.page_cache,
            tx_ctx.new_version,
        );
    }

    pub fn commit(&mut self, tx_ctx: &mut TxCtx) {
        self.finalise_db_changes(
            &mut tx_ctx.master_page,
            tx_ctx.new_version,
            tx_ctx.global_root_page_no,
            tx_ctx.tree_dir_root_page_no,
            &mut tx_ctx.free_page_tracker,
        );
    }

    // After completing updates to the tree need to finalise the changes
    // to the database.
    // This means:
    //    Writing the free page directory back to out.
    //    Updating the master page with the new tree roots and new
    //    free page directory.
    //    Sync the file.
    //    Write the master page to file overwriting the flipping
    //    the master pages.
    //    Sync the master page.
    fn finalise_db_changes(
        &mut self,
        master_page: &mut DbMasterPage,
        new_version: u64,
        new_root_page_no: u64,
        new_table_tree_root_no: u64,
        free_page_tracker: &mut FreePageTracker,
    ) {
        // Write out the free pages.
        // Write the new free page directory back through the page cache.
        let mut free_dir_pages = free_page_tracker.get_free_dir_pages(&mut self.page_cache);
        assert!(!free_dir_pages.is_empty());
        let first_free_dir_page = free_dir_pages.last().unwrap().get_page_number();
        while let Some(mut free_dir_page) = free_dir_pages.pop() {
            self.page_cache.put_page(free_dir_page.get_page());
        }

        // Now need to update the master - update the following:
        //   - The global tree root page.
        //   - The table directory tree.
        //   - The free page directory.
        //   - The new version.
        master_page.set_free_page_dir_page_no(first_free_dir_page.to_u64());
        master_page.set_global_tree_root_page_no(new_root_page_no);
        master_page.set_table_dir_page_no(new_table_tree_root_no);
        master_page.set_version(new_version);

        // Flip the page number to overrwrite the non-current master
        // page and make it the new current master.
        master_page.flip_page_number();

        // Sync all pages except the master, which has not been written yet.
        self.page_cache.sync_data();
        // Put the master page.
        self.page_cache.put_page(master_page.get_page());
        // Now sync the master
        self.page_cache.sync_data();
    }

    pub fn get_table_tree_root(&mut self, name: &[u8]) -> Option<u64> {
        let tx_ctx = self.new_transaction();
        self.get_table_tree_root_txn(name, &tx_ctx)
    }

    // Get the root page number for a table tree if it exists.
    pub fn get_table_tree_root_txn(&mut self, name: &[u8], tx_ctx: &TxCtx) -> Option<u64> {
        assert!(
            name.len() < u8::MAX as usize,
            "Cannot handle keys larger than u8::MAX."
        );
        let table_dir_page_no = tx_ctx.tree_dir_root_page_no;

        if let Some(tuple) =
            StoreTupleProcessor::get_tuple(name, table_dir_page_no, &mut self.page_cache)
        {
            assert!(tuple.get_overflow() == Overflow::None);
            assert_eq!(tuple.get_value().len(), 8);
            let page_no = u64::from_le_bytes(tuple.get_value().try_into().unwrap());
            Some(page_no)
        } else {
            None
        }
    }

    pub fn put_table_entry(&mut self, table_name: &[u8], key: &[u8], value: &[u8]) {
        let mut tx_ctx = self.new_transaction();
        self.put_table_entry_txn(table_name, key, value, &mut tx_ctx);
        self.commit(&mut tx_ctx);
    }

    // Put a key value into a table. If the table does not exist then create it.
    pub fn put_table_entry_txn(
        &mut self,
        table_name: &[u8],
        key: &[u8],
        value: &[u8],
        tx_ctx: &mut TxCtx,
    ) {
        assert!(
            table_name.len() < u8::MAX as usize,
            "Cannot handle table name larger than u8::MAX."
        );

        let mut table_root_page_no_wrapped = self.get_table_tree_root_txn(table_name, tx_ctx);
        if table_root_page_no_wrapped.is_none() {
            // Note this is a transaction on its own, ie the master
            // page is overrwritten. Could do all this in a single
            // transaction.
            self.create_table_txn(table_name, tx_ctx);
            table_root_page_no_wrapped = self.get_table_tree_root_txn(table_name, tx_ctx);
        }
        let table_root_page = table_root_page_no_wrapped.unwrap();

        // Create the tuple we want to add.
        // If key/value are large then this could be an overflow tuple
        // with the ley/value stored in overflow pages by this method.
        let tuple = TupleProcessor::generate_tuple(
            key,
            value,
            &mut self.page_cache,
            &mut tx_ctx.free_page_tracker,
            tx_ctx.new_version,
            &self.compressor,
        );

        // Store the tuple in the table's tree, this will return
        // the new root page number for the table tree.
        let table_root_page = self.page_cache.get_page(PageNo::from_u64(table_root_page));
        let new_table_root_page_no = StoreTupleProcessor::store_tuple(
            tuple,
            table_root_page,
            &mut tx_ctx.free_page_tracker,
            &mut self.page_cache,
            tx_ctx.new_version,
        );

        // Need to update the table directory tree with the new root
        // for the table tree, create a tuple for the new tree reference.
        let table_tuple = TupleProcessor::generate_tuple(
            table_name,
            &new_table_root_page_no.to_le_bytes(),
            &mut self.page_cache,
            &mut tx_ctx.free_page_tracker,
            tx_ctx.new_version,
            &self.compressor,
        );

        // Now store the new table reference into the table directory tree.
        let table_dir_root_page_no = tx_ctx.tree_dir_root_page_no;
        let table_dir_root_page = self
            .page_cache
            .get_page(PageNo::from_u64(table_dir_root_page_no));
        let new_table_dir_root_page_no = StoreTupleProcessor::store_tuple(
            table_tuple,
            table_dir_root_page,
            &mut tx_ctx.free_page_tracker,
            &mut self.page_cache,
            tx_ctx.new_version,
        );
        tx_ctx.tree_dir_root_page_no = new_table_dir_root_page_no;
    }

    // Remove all the entries in a table.
    pub fn clear_table(&mut self, table_name: &[u8]) {
        self.clear_table_with_delete(table_name, false);
    }

    // Clear a table then remove it from the table directory
    // tree.
    pub fn delete_table(&mut self, table_name: &[u8]) {
        self.clear_table_with_delete(table_name, true);
    }

    pub fn clear_table_with_delete(&mut self, table_name: &[u8], delete: bool) {
        let mut tx_ctx = self.new_transaction();
        self.clear_table_with_delete_txn(table_name, delete, &mut tx_ctx);
        self.commit(&mut tx_ctx);
    }

    // Clear the contents of a table. If delete is true then the table will be deleted, if false
    // then the table will be cleared but remain in place.
    //
    pub fn clear_table_with_delete_txn(
        &mut self,
        table_name: &[u8],
        delete: bool,
        tx_ctx: &mut TxCtx,
    ) {
        assert!(
            table_name.len() < u8::MAX as usize,
            "Cannot handle table name larger than u8::MAX."
        );

        let table_root_page_no_wrapped = self.get_table_tree_root_txn(table_name, tx_ctx);
        if table_root_page_no_wrapped.is_none() {
            // No table to clear or delete.
            return;
        }
        let table_root_page = table_root_page_no_wrapped.unwrap();

        // First clear the table tree.
        let table_root_page = self.page_cache.get_page(PageNo::from_u64(table_root_page));
        let new_table_root_page_no = ClearHandler::clear_tree(
            table_root_page,
            &mut tx_ctx.free_page_tracker,
            &mut self.page_cache,
            tx_ctx.new_version,
        );

        // Now need to update the table directory tree.
        let table_dir_root_page_no = tx_ctx.tree_dir_root_page_no;
        let table_dir_root_page = self
            .page_cache
            .get_page(PageNo::from_u64(table_dir_root_page_no));

        // If the table is to be deleted, then delete the table key/name
        // from the table directory tree.
        let new_table_dir_root_page_no: u64 = if delete {
            tx_ctx
                .free_page_tracker
                .return_free_page_no(PageNo::from_u64(new_table_root_page_no));
            let (new_page, _is_deleted) = TreeDeleteHandler::delete_key(
                table_name,
                table_dir_root_page,
                &mut self.page_cache,
                &mut tx_ctx.free_page_tracker,
                tx_ctx.new_version,
            );
            // Page number of the new root of the table directory tree.
            new_page
        } else {
            // Not deleting the table, need to update its
            // reference to the new table tree root page.
            //
            // Create new table reference.
            let table_tuple = TupleProcessor::generate_tuple(
                table_name,
                &new_table_root_page_no.to_le_bytes(),
                &mut self.page_cache,
                &mut tx_ctx.free_page_tracker,
                tx_ctx.new_version,
                &self.compressor,
            );
            // Store table reference and provide the
            // new table directory tree root page.
            StoreTupleProcessor::store_tuple(
                table_tuple,
                table_dir_root_page,
                &mut tx_ctx.free_page_tracker,
                &mut self.page_cache,
                tx_ctx.new_version,
            )
        };
        tx_ctx.tree_dir_root_page_no = new_table_dir_root_page_no;
    }

    // Get a value from a table tree.
    pub fn get_table_entry(&mut self, table_name: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        // Name size check handled in get_table_tree_root
        let table_root_page_no_wrapped = self.get_table_tree_root(table_name);
        // If the table does not exist could throw an error.
        table_root_page_no_wrapped?;

        let table_root_page_no = table_root_page_no_wrapped.unwrap();
        self.get_from_tree(key, table_root_page_no)
    }

    pub fn get_table_entry_txn(
        &mut self,
        table_name: &[u8],
        key: &[u8],
        tx_ctx: &TxCtx,
    ) -> Option<Vec<u8>> {
        // Name size check handled in get_table_tree_root
        let table_root_page_no_wrapped = self.get_table_tree_root_txn(table_name, tx_ctx);
        // If the table does not exist could throw an error.
        table_root_page_no_wrapped?;

        let table_root_page_no = table_root_page_no_wrapped.unwrap();
        self.get_from_tree(key, table_root_page_no)
    }

    pub fn delete_table_entry(&mut self, table_name: &[u8], key: &[u8]) -> bool {
        let mut tx_ctx = self.new_transaction();
        let deleted = self.delete_table_entry_txn(table_name, key, &mut tx_ctx);
        self.commit(&mut tx_ctx);
        deleted
    }

    // Delete an entry from a table tree.
    pub fn delete_table_entry_txn(
        &mut self,
        table_name: &[u8],
        key: &[u8],
        tx_ctx: &mut TxCtx,
    ) -> bool {
        // Get the root of the table's tree.
        // Name size check handled in get_table_tree_root
        let table_root_page_no_wrapped = self.get_table_tree_root_txn(table_name, tx_ctx);
        if table_root_page_no_wrapped.is_none() {
            // table does not exist - should maybe throw error
            return false;
        }
        let table_root_page_no = table_root_page_no_wrapped.unwrap();

        // If its an oversized key then need to generate a short one key for it.
        // The short key is the first 223 bytes of the key followed by the
        // SHA256 of the whole key.
        let key_to_use: Vec<u8> = if TupleProcessor::is_oversized_key(key) {
            TupleProcessor::generate_short_key(key)
        } else {
            key.to_owned()
        };

        // Delete the key from the table tree and get back the new root page
        // number of the table tree.
        let root_page = self
            .page_cache
            .get_page(PageNo::from_u64(table_root_page_no));
        let (new_tree_free_page_no, deleted) = TreeDeleteHandler::delete_key(
            &key_to_use,
            root_page,
            &mut self.page_cache,
            &mut tx_ctx.free_page_tracker,
            tx_ctx.new_version,
        );
        if !deleted {
            // No changes to DB needed
            return false;
        }

        // The table tree has been updated, need to update the
        // table directory tree with the new root page for the
        // table tree.
        let table_tuple = TupleProcessor::generate_tuple(
            table_name,
            &new_tree_free_page_no.to_le_bytes(),
            &mut self.page_cache,
            &mut tx_ctx.free_page_tracker,
            tx_ctx.new_version,
            &self.compressor,
        );

        // Get the table directory tree.
        let table_dir_root_page_no = tx_ctx.tree_dir_root_page_no;
        let table_dir_root_page = self
            .page_cache
            .get_page(PageNo::from_u64(table_dir_root_page_no));
        // Update the reference for the table tree.
        let new_table_dir_root_page_no = StoreTupleProcessor::store_tuple(
            table_tuple,
            table_dir_root_page,
            &mut tx_ctx.free_page_tracker,
            &mut self.page_cache,
            tx_ctx.new_version,
        );
        tx_ctx.tree_dir_root_page_no = new_table_dir_root_page_no;
        deleted
    }
}

// Functions to either create or to initialise the database.
impl Db {
    fn check_db_integrity(&mut self) -> std::io::Result<()> {
        let root_page = DbRootPage::from_page(self.page_cache.get_page(PageNo::from_u64(0)));
        // There is no sanity check for sanity type, if the db was created with
        // encryption and then opened without a key then we will not be able to open
        // the root_page as the checksum will not match.
        // This could be avoided if the root page was not encrypted.
        let stored_compressor_type = CompressorType::try_from(root_page.get_compression_type())
            .expect("Unknown compressoion");
        if stored_compressor_type != self.compressor.compressor_type {
            panic!(
                "Db compression mis-match, stored type is {:?}, requested type {:?}",
                root_page.get_compression_type(),
                self.compressor.compressor_type
            );
        }
        // Get the two master pages.
        let master_page1 = DbMasterPage::from_page(self.page_cache.get_page(PageNo::from_u64(1)));
        let master_page2 = DbMasterPage::from_page(self.page_cache.get_page(PageNo::from_u64(2)));
        // Determine which is the current master.
        let current_master = if master_page1.get_version() > master_page2.get_version() {
            master_page1
        } else {
            master_page2
        };
        let current_version = current_master.get_version();
        // Check the free_dir_page is sane.
        let free_dir_page_no = current_master.get_free_page_dir_page_no();
        let free_dir_page =
            FreeDirPage::from_page(self.page_cache.get_page(PageNo::from_u64(free_dir_page_no)));
        assert!(free_dir_page.get_version() <= current_version);

        Ok(())
    }

    // There is no DB file, or the file is empty.
    // Need to create pages and then write the
    // initial meta data pages.
    fn init_db_file(&mut self, sanity_type: BlockSanity) -> std::io::Result<()> {
        // Get some free pages and make space in the file.
        // Will trigger a file sync.
        // Provides a list of free pages that can be modified or added
        // to the free page directory if not used in the init process -
        // the init process will generate some unused pages.
        let mut free_pages: Vec<PageNo> = self.page_cache.generate_free_pages(10, 0);
        assert!(free_pages.len() == 10);

        // Write the global tree root page at page number 5.
        // The first page in a tree is a leaf page.
        let mut global_tree_root_page =
            LeafPage::create_new(self.page_cache.get_page_config(), 5, 0);
        // remove it from the free list
        free_pages.retain(|&x| x.get_blk_offset() != 5);
        // Write the global_tree_root_page to disk.
        self.page_cache.put_page(global_tree_root_page.get_page());

        // Write the table directory page at page number 4.
        // The first page in a tree is a leaf page.
        let mut table_dir_page = LeafPage::create_new(self.page_cache.get_page_config(), 4, 0);
        // remove from the free page list
        free_pages.retain(|&x| x.get_blk_offset() != 4);
        self.page_cache.put_page(table_dir_page.get_page());

        // Write first master page at page number 1.
        let mut master_page1: DbMasterPage =
            DbMasterPage::create_new(self.page_cache.get_page_config(), PageNo::new(0, 1), 0);
        // remove from free page list
        free_pages.retain(|&x| x.get_blk_offset() != 1);
        // Tell the first master page where the free page directory page is,
        // where the table directory root page is and where the global
        // tree root is.
        master_page1.set_free_page_dir_page_no(3);
        master_page1.set_table_dir_page_no(4);
        master_page1.set_global_tree_root_page_no(5);
        self.page_cache.put_page(master_page1.get_page());

        // Write second master page at page number 2, the version
        // is 1 - this makes master_page2 the current master page.
        let mut master_page2: DbMasterPage =
            DbMasterPage::create_new(self.page_cache.get_page_config(), PageNo::new(0, 2), 1);
        // remove from free page list
        free_pages.retain(|&x| x.get_blk_offset() != 2);
        master_page2.set_free_page_dir_page_no(3);
        master_page2.set_table_dir_page_no(4);
        master_page2.set_global_tree_root_page_no(5);
        self.page_cache.put_page(master_page2.get_page());

        // Now write the free page directory at page 3.
        let mut free_dir_page = FreeDirPage::create_new(self.page_cache.get_page_config(), 3, 0);
        // The free_dir_page is no longer free, and also the root db page won't be free after
        // we write it in the next step.
        free_pages.retain(|&x| x.get_blk_offset() != 0);
        free_pages.retain(|&x| x.get_blk_offset() != 3);
        free_dir_page.add_free_pages(&free_pages);
        self.page_cache.put_page(free_dir_page.get_page());

        // Flush all pages so far, don't sync the db metadata page yet.
        self.page_cache.sync_data();
        // All pages except the metadata page are written, however the
        // the DB is not sane until the next step.

        // Write the root page as last step to make the DB sane.
        let mut db_root_page: DbRootPage =
            DbRootPage::create_new(self.page_cache.get_page_config());
        db_root_page.set_sanity_type(sanity_type);
        db_root_page.set_compression_type(self.compressor.compressor_type.clone().into());
        self.page_cache.put_page(db_root_page.get_page());

        assert!(free_pages.len() == 4, "There should be 4 free pages");

        self.page_cache.sync_data();
        Ok(())
    }
}

impl Db {
    // Returns the current master depending on which has the highest
    // version.
    // An update will follow this pattern:
    //   Get current master page based on version.
    //   Generate a new version number.
    //   Update the master page after making tree changes.
    //   Overwrite the non-current master page with the new version.
    fn get_master_page(&mut self) -> DbMasterPage {
        let master_page1 = DbMasterPage::from_page(self.page_cache.get_page(PageNo::from_u64(1)));
        let master_page2 = DbMasterPage::from_page(self.page_cache.get_page(PageNo::from_u64(2)));

        if master_page1.get_version() > master_page2.get_version() {
            master_page1
        } else {
            master_page2
        }
    }

    // A tuple may be compressed, uncompress if necessary.
    // Should this be with the tuple code?
    fn get_tuple_value<T: TupleTrait>(&self, tuple: &T) -> Vec<u8> {
        let overflow = tuple.get_overflow();
        if overflow == Overflow::ValueCompressed || overflow == Overflow::KeyValueCompressed {
            return self.compressor.decompress(tuple.get_value());
        }
        // Return a copy of the tuple. This is now a copy of a copy!?
        tuple.get_value().to_vec()
    }

    // Get the key - it could be compressed in which case it needs to be
    // uncompressed.
    // Should this be with the tuple code?
    fn get_tuple_key<T: TupleTrait>(&self, tuple: &T) -> Vec<u8> {
        let overflow = tuple.get_overflow();
        if overflow == Overflow::KeyValueCompressed {
            return self.compressor.decompress(tuple.get_key());
        }
        tuple.get_key().to_vec()
    }
}

impl Drop for Db {
    // This is a bit weird - the file is not closed as
    // this is done via the file object drop.
    // Not sure this is necessary.
    fn drop(&mut self) {
        self.page_cache.sync_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rng, seq::SliceRandom};
    use std::fs;
    use tempfile::NamedTempFile;

    #[test]
    fn test_db_creation() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        {
            Db::new(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
            );
        }
        {
            let mut db = Db::new(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
            );
            let _head_page1 = DbMasterPage::from_page(db.page_cache.get_page(PageNo::from_u64(1)));
            let head_page2 = DbMasterPage::from_page(db.page_cache.get_page(PageNo::from_u64(2)));
            let free_page_dir_page_no = head_page2.get_free_page_dir_page_no();
            let free_page_dir_page = FreeDirPage::from_page(
                db.page_cache
                    .get_page(PageNo::from_u64(free_page_dir_page_no)),
            );
            assert!(free_page_dir_page.get_entries() == 4);
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

    #[test]
    fn test_db_store_value() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let key = b"the_key".to_vec();
        let value = b"the_value".to_vec();
        {
            let mut db = Db::new(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
            );
            assert!(!db.delete(&key));
            db.put(key.as_ref(), value.as_ref());
        }
        // The new scope essentially closes the DB - when Files run out of scope then
        // they are close, Rust bizairely does not allow error handling on close!
        {
            let mut db = Db::new(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
            );
            let returned_value = db.get(key.as_ref()).unwrap();
            assert!(returned_value == value);
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

    #[test]
    fn test_db_store_two_value() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let key = b"the_key".to_vec();
        let value = b"the_value".to_vec();
        let another_key = b"another_key".to_vec();
        let another_value = b"another_value".to_vec();
        {
            let mut db = Db::new(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
            );
            db.put(key.as_ref(), value.as_ref());
            db.put(another_key.as_ref(), another_value.as_ref());
        }
        // The new scope essentially closes the DB - when Files run out of scope then
        // they are close, Rust bizairely does not allow error handling on close!
        {
            let mut db = Db::new(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
            );
            let returned_value = db.get(key.as_ref()).unwrap();
            assert!(returned_value == value);
            let returned_value = db.get(another_key.as_ref()).unwrap();
            assert!(returned_value == another_value);
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

    #[test]
    fn test_db_store_value_delete() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let key = b"the_key".to_vec();
        let value = b"the_value".to_vec();
        {
            let mut db = Db::new(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
            );
            db.put(key.as_ref(), value.as_ref());
        }
        // The new scope essentially closes the DB - when Files run out of scope then
        // they are close, Rust bizairely does not allow error handling on close!
        {
            let mut db = Db::new(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
            );
            let returned_value = db.get(key.as_ref()).unwrap();
            assert!(returned_value == value);
        }
        {
            let mut db = Db::new(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
            );
            let deleted = db.delete(key.as_ref());
            assert!(deleted);
        }
        {
            let mut db = Db::new(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
            );
            let returned_value = db.get(key.as_ref());
            assert!(returned_value.is_none());
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

    #[test]
    fn test_db_store_value_delete_small_page_reverse() {
        let size = 4096u64;
        let block_size = 256;
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            for i in 0u64..=size {
                db.put(&i.to_be_bytes(), &i.to_be_bytes());
            }
        }
        // The new scope essentially closes the DB - when Files run out of scope then
        // they are close, Rust bizairely does not allow error handling on close!
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            for i in 0u64..=size {
                let returned_value = db.get(&i.to_be_bytes()).unwrap();
                assert_eq!(u64::from_be_bytes(returned_value.try_into().unwrap()), i);
            }
        }
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            for i in (0..(size + 1)).rev() {
                let returned_value = db.get(&i.to_be_bytes()).unwrap();
                assert_eq!(u64::from_be_bytes(returned_value.try_into().unwrap()), i);
                let deleted = db.delete(&i.to_be_bytes());
                if !deleted {
                    assert!(deleted);
                }
                let returned_value = db.get(&i.to_be_bytes());
                assert!(returned_value.is_none());
            }
        }
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            let i: u64 = 0;
            let returned_value = db.get(&i.to_be_bytes());
            assert!(returned_value.is_none());
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

    #[test]
    fn test_db_store_value_delete_small_page_reverse_le() {
        let block_size = 256;
        let size = 4096u64;
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            for i in 0u64..=size {
                db.put(&i.to_le_bytes(), &i.to_le_bytes());
            }
        }
        // The new scope essentially closes the DB - when Files run out of scope then
        // they are close, Rust bizairely does not allow error handling on close!
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            for i in 0u64..=size {
                let returned_value = db.get(&i.to_le_bytes()).unwrap();
                assert_eq!(u64::from_le_bytes(returned_value.try_into().unwrap()), i);
            }
        }
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            for i in (0..(size + 1)).rev() {
                let returned_value = db.get(&i.to_le_bytes()).unwrap();
                assert_eq!(u64::from_le_bytes(returned_value.try_into().unwrap()), i);
                let deleted = db.delete(&i.to_le_bytes());
                if !deleted {
                    assert!(deleted);
                }
                let returned_value = db.get(&i.to_le_bytes());
                assert!(returned_value.is_none());
            }
        }
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            let i: u64 = 0;
            let returned_value = db.get(&i.to_le_bytes());
            assert!(returned_value.is_none());
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

    #[test]
    fn test_db_store_value_delete_small_page_random() {
        let size = 4096u64;
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                256,
            );
            let deleted = db.delete(&0u64.to_be_bytes());
            assert!(!deleted);
            let mut numbers: Vec<u64> = (0..=size).collect();
            let mut rng = rng();
            numbers.shuffle(&mut rng);
            for i in numbers {
                db.put(&i.to_be_bytes(), &i.to_be_bytes());
            }
            let deleted = db.delete(&6400u64.to_be_bytes());
            assert!(!deleted);
        }
        // The new scope essentially closes the DB - when Files run out of scope then
        // they are close, Rust bizairely does not allow error handling on close!
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                256,
            );
            for i in 0u64..=size {
                let returned_value = db.get(&i.to_be_bytes()).unwrap();
                assert_eq!(u64::from_be_bytes(returned_value.try_into().unwrap()), i);
            }
        }
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                256,
            );
            let mut numbers: Vec<u64> = (0..=size).collect();
            let mut rng = rng();
            numbers.shuffle(&mut rng);
            for i in numbers {
                let returned_value = db.get(&i.to_be_bytes()).unwrap();
                assert_eq!(u64::from_be_bytes(returned_value.try_into().unwrap()), i);
                let deleted = db.delete(&i.to_be_bytes());
                assert!(deleted);
                let returned_value = db.get(&i.to_be_bytes());
                assert!(returned_value.is_none());
            }
        }
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                256,
            );
            let i: u64 = 0;
            let returned_value = db.get(&i.to_be_bytes());
            assert!(returned_value.is_none());
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

    #[test]
    fn test_db_store_value_delete_small_page_random_le() {
        let size = 4096u64;
        let block_size = 256;
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            let mut numbers: Vec<u64> = (0..=size).collect();
            let mut rng = rng();
            numbers.shuffle(&mut rng);
            for i in numbers {
                db.put(&i.to_le_bytes(), &i.to_le_bytes());
            }
        }
        // The new scope essentially closes the DB - when Files run out of scope then
        // they are close, Rust bizairely does not allow error handling on close!
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            for i in 0u64..=size {
                let returned_value = db.get(&i.to_le_bytes()).unwrap();
                assert_eq!(u64::from_le_bytes(returned_value.try_into().unwrap()), i);
            }
        }
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            let mut numbers: Vec<u64> = (0..=size).collect();
            let mut rng = rng();
            numbers.shuffle(&mut rng);
            for i in numbers {
                let returned_value = db.get(&i.to_le_bytes()).unwrap();
                assert_eq!(u64::from_le_bytes(returned_value.try_into().unwrap()), i);
                let deleted = db.delete(&i.to_le_bytes());
                assert!(deleted);
                let returned_value = db.get(&i.to_le_bytes());
                assert!(returned_value.is_none());
            }
        }
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            let i: u64 = 0;
            let returned_value = db.get(&i.to_le_bytes());
            assert!(returned_value.is_none());
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

    #[test]
    fn test_db_store_value_delete_overflow() {
        let size = 40u64;
        let value = vec![0u8; 2048];
        let key = [0u8; 200];
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                4096,
            );
            let mut numbers: Vec<u64> = (0..=size).collect();
            let mut rng = rng();
            numbers.shuffle(&mut rng);
            for i in numbers {
                let mut k = key.to_vec();
                k[0..8].copy_from_slice(&i.to_le_bytes());
                db.put(&k, &value);
            }
        }
        // The new scope essentially closes the DB - when Files run out of scope then
        // they are close, Rust bizairely does not allow error handling on close!
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                4096,
            );
            for i in 0u64..=size {
                let mut k = key.to_vec();
                k[0..8].copy_from_slice(&i.to_le_bytes());
                let returned_value = db.get(&k);
                assert!(returned_value.is_some());
            }
        }
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                4096,
            );
            let mut numbers: Vec<u64> = (0..=size).collect();
            let mut rng = rng();
            numbers.shuffle(&mut rng);
            for i in numbers {
                let mut k = key.to_vec();
                k[0..8].copy_from_slice(&i.to_le_bytes());
                let deleted = db.delete(&k);
                assert!(deleted);
                let returned_value = db.get(&k);
                assert!(returned_value.is_none());
            }
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

    #[test]
    fn test_db_clear() {
        let block_size = 256;
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            let mut numbers: Vec<u64> = (0..=256).collect();
            let mut rng = rng();
            numbers.shuffle(&mut rng);
            for i in numbers {
                db.put(&i.to_be_bytes(), &i.to_be_bytes());
            }
        }
        // The new scope essentially closes the DB - when Files run out of scope then
        // they are close, Rust bizairely does not allow error handling on close!
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            db.clear();
            let i: u64 = 0;
            let returned_value = db.get(&i.to_be_bytes());
            assert!(returned_value.is_none());
        }
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            let mut numbers: Vec<u64> = (0..=256).collect();
            let mut rng = rng();
            numbers.shuffle(&mut rng);
            for i in numbers {
                let returned_value = db.get(&i.to_be_bytes());
                assert!(returned_value.is_none());
            }
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

    #[test]
    fn test_match() {
        use std::cmp::Ordering;
        let mid: &[u8] = &[0, 0, 0, 0, 0, 0, 2, 1];
        let split: &[u8] = &[0, 0, 0, 0, 0, 0, 2];
        let key: &[u8] = &[0, 0, 0, 0, 0, 0, 2, 0];

        let result = key.cmp(split);
        assert_eq!(result, Ordering::Greater);
        let result = split.cmp(key);
        assert_eq!(result, Ordering::Less);
        let result2 = key.cmp(mid);
        assert_eq!(result2, Ordering::Less);
    }

    #[test]
    fn test_db_store_value_delete_small_page() {
        let size = 4096u64;
        let block_size = 256;
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            for i in 0u64..size {
                db.put(&i.to_be_bytes(), &i.to_be_bytes());
                for j in 0u64..i {
                    let returned_value = db.get(&j.to_be_bytes());
                    assert!(returned_value.is_some());
                }
            }
        }
        // The new scope essentially closes the DB - when Files run out of scope then
        // they are close, Rust bizairely does not allow error handling on close!
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            for i in 0u64..size {
                let returned_value = db.get(&i.to_be_bytes());
                if returned_value.is_none() {
                    assert!(returned_value.is_some());
                }
                assert_eq!(
                    u64::from_be_bytes(returned_value.unwrap().try_into().unwrap()),
                    i
                );
            }
        }
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            for i in 0u64..size {
                let returned_value = db.get(&i.to_be_bytes()).unwrap();
                assert_eq!(u64::from_be_bytes(returned_value.try_into().unwrap()), i);
                let deleted = db.delete(&i.to_be_bytes());
                if !deleted {
                    assert!(deleted);
                }
                let returned_value = db.get(&i.to_be_bytes());
                assert!(returned_value.is_none());
            }
        }
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            let i: u64 = 0;
            let returned_value = db.get(&i.to_be_bytes());
            assert!(returned_value.is_none());
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }

    #[test]
    fn test_db_store_value_delete_small_page_little_endian() {
        let size = 4096u64;
        let block_size = 256;
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            for i in 0u64..size {
                db.put(&i.to_le_bytes(), &i.to_le_bytes());
                for j in 0u64..i {
                    let returned_value = db.get(&j.to_le_bytes());
                    if returned_value.is_none() {
                        assert!(returned_value.is_some());
                    }
                }
            }
        }
        // The new scope essentially closes the DB - when Files run out of scope then
        // they are close, Rust bizairely does not allow error handling on close!
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            for i in 0u64..size {
                let returned_value = db.get(&i.to_le_bytes());
                if returned_value.is_none() {
                    assert!(returned_value.is_some());
                }
                assert_eq!(
                    u64::from_le_bytes(returned_value.unwrap().try_into().unwrap()),
                    i
                );
            }
        }
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            for i in 0u64..size {
                let returned_value = db.get(&i.to_le_bytes()).unwrap();
                assert_eq!(u64::from_le_bytes(returned_value.try_into().unwrap()), i);
                let deleted = db.delete(&i.to_le_bytes());
                if !deleted {
                    assert!(deleted);
                }
                let returned_value = db.get(&i.to_le_bytes());
                assert!(returned_value.is_none());
            }
        }
        {
            let mut db = Db::new_with_page_size(
                temp_file.path().to_str().unwrap(),
                None,
                CompressorType::None,
                block_size,
            );
            let i: u64 = 0;
            let returned_value = db.get(&i.to_le_bytes());
            assert!(returned_value.is_none());
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }
}
