pub struct Db {
    path: String,
    file: Option<std::fs::File>,
}


impl Db {
    pub const PAGE_SIZE: u64 = 4096;
    pub const MAGIC_NUMBER: u32 = 26061973;

    pub fn new(path: &str) -> Self {
        Db {
            path: path.to_string(),
            file: None,
        }
    }

    pub fn init(&mut self) -> std::io::Result<()> {
        use std::fs::OpenOptions;
        use std::path::Path;

        if Path::new(&self.path).exists() {
            self.file = Some(OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.path).expect("Failed to open existing DB file"));

            self.check_db_integrity().expect("DB integrity check failed");
            return Ok(());
        }

        self.file = Some(OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&self.path).expect("Failed to open or create DB file"));

        self.init_db_file().expect("Failed to initialize DB file");
        Ok(())
    }

    pub fn check_db_integrity(&mut self) -> std::io::Result<()> {
        let mut buffer = vec![0u8; Self::PAGE_SIZE as usize];
        self.read_page(0, &mut buffer).expect("Failed to read DB header page");
        let magic = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        assert_eq!(magic, Self::MAGIC_NUMBER, "Invalid DB magic number");
        Ok(())
    }

    pub fn init_db_file(&mut self) -> std::io::Result<()> {
        self.write_db_header().expect("Failed to write DB header");
        Ok(())
    }

    pub fn close(&self) -> std::io::Result<()> {
        if let Some(f) = &self.file {
            f.sync_all().expect("Failed to sync DB file to disk");
        }   
        Ok(())
    }

    pub fn write_db_header(&mut self) -> std::io::Result<()> {   
        use std::io::Cursor;
        use byteorder::{LittleEndian, WriteBytesExt};
        
        let mut page_data = vec![0u8; Self::PAGE_SIZE as usize];
        let mut cursor = Cursor::new(&mut page_data[..]);
        cursor.write_u32::<LittleEndian>(Self::MAGIC_NUMBER).expect("Should be able to write magic number");
        self.write_page(0, &page_data).expect("Should be able to write DB page");

        Ok(())
    }  

    pub fn write_page(&mut self, page_number: u64, page_data: &[u8]) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};

        let ref mut f = self.file.as_mut().expect("DB file not initialized");
        let offset = page_number * Self::PAGE_SIZE;
        f.seek(SeekFrom::Start(offset)).expect("Failed to seek to DB page offset");
        f.write_all(page_data).expect("Failed to write DB page");
        f.sync_all().expect("Failed to sync DB page to disk");
        
        Ok(())
    }

    pub fn read_page(&mut self, page_number: u64, buffer: &mut [u8]) -> std::io::Result<()> {
        use std::io::{Seek, SeekFrom, Read};

        let ref mut f = self.file.as_mut().expect("DB file not initialized");
        let offset = page_number * Self::PAGE_SIZE;
        f.seek(SeekFrom::Start(offset)).expect("Failed to seek to DB page offset");
        f.read_exact(buffer).expect("Failed to read DB page");
        
        Ok(())
    }
}

    #[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_db_init_creates_file() {
        let path = "/tmp/test_db_init.db";
        let mut db = Db::new(path);
        let _ = std::fs::remove_file(path); // Clean up before test
        assert!(db.init().is_ok());
        assert!(std::path::Path::new(path).exists());
        db.close().expect("Failed to close DB");

        db = Db::new(path);
        assert!(db.init().is_ok()); // Re-initialize should succeed
        db.close().expect("Failed to close DB");
        let _ = std::fs::remove_file(path); // Clean up after test
    }

    #[test]
    fn test_db_close_syncs_file() {
        let path = "/tmp/test_db_close.db";
        let mut db = Db::new(path);
        let _ = std::fs::remove_file(path);
        db.init().unwrap();
        assert!(db.close().is_ok());
        let _ = std::fs::remove_file(path);
    }
}