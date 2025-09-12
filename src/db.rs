pub struct Db {
    path: String,
    file: Option<std::fs::File>,
}


impl Db {
    pub fn new(path: &str) -> Self {
        Db {
            path: path.to_string(),
            file: None,
        }
    }

    pub fn init(&mut self) -> std::io::Result<()> {
        use std::fs::OpenOptions;

        self.file = Some(OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.path).expect("Failed to open or create DB file"));

        Ok(())
    }

    pub fn close(&self) -> std::io::Result<()> {
        if let Some(f) = &self.file {
            f.sync_all()?;
        }   
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