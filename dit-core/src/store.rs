use crate::peer::DhtAddr;
use std::fs::{self, File};
use std::io::{self, Seek};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreConfig {
    /// Location of the store.
    pub dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct Store {
    config: StoreConfig,
}

impl Store {
    /// Opens the dit store, creating the directory structure if needed.
    pub fn open(config: StoreConfig) -> io::Result<Self> {
        fs::create_dir_all(config.dir.join("blobs"))?;
        Ok(Self { config })
    }

    /// Copies a file into the store without notifying peers.
    pub fn add_file(&self, src_path: impl AsRef<Path>) -> io::Result<DhtAddr> {
        let mut src_file = File::open(&src_path)?;
        let hash = DhtAddr::hash_reader(&mut src_file)?;

        let dst_path = self.blob_path(hash);
        let mut dst_file = File::create(dst_path)?;
        src_file.rewind()?;
        io::copy(&mut src_file, &mut dst_file)?;

        Ok(hash)
    }

    /// Opens a file from the blob store by using the file's hash value.
    pub fn open_file(&self, file_hash: DhtAddr) -> io::Result<File> {
        File::open(self.blob_path(file_hash))
    }

    /// Delete a file from the blob store with the file hash.
    pub fn remove_file(&self, file_hash: DhtAddr) -> io::Result<()> {
        fs::remove_file(self.blob_path(file_hash))
    }

    fn blob_path(&self, hash: DhtAddr) -> PathBuf {
        let mut dst_path = self.config.dir.join("blobs");
        dst_path.push(hash.to_string());
        dst_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Read, Write};

    #[test]
    fn add_open_remove() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("hello.txt");
        let store_dir = temp_dir.path().join("store");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(b"hello world").unwrap();
        drop(file);

        // Opening store creates the store dir.
        assert!(!store_dir.exists());
        let config = StoreConfig {
            dir: store_dir.clone(),
        };
        let store = Store::open(config).unwrap();
        assert!(store_dir.exists());

        // Add file to store.
        let hash = store.add_file(&file_path).unwrap();
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
                .parse()
                .unwrap(),
        );

        // Adding a file again overwrites the old one.
        let hash2 = store.add_file(&file_path).unwrap();
        assert_eq!(hash, hash2);

        // Open file from hash.
        let mut file = store.open_file(hash).unwrap();
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();
        assert_eq!(content, "hello world");
        drop(file);

        // Remove file.
        store.remove_file(hash).unwrap();
        store.open_file(hash).unwrap_err();
        store.remove_file(hash).unwrap_err();
    }
}
