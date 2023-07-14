use std::fs;
use std::io;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use crate::peer::DhtAddr;

#[derive(Clone)]
pub struct Store {
    /// Location of the store.
    dir: PathBuf,
    blob_dir: PathBuf,
}

impl Store {
    /// Open dit path, creating blob_dir for local added files
    pub fn open(dir: impl AsRef<Path>) -> Self {
        Self {
            dir: dir.as_ref().to_owned(),
            blob_dir: dir.as_ref().join("blobs"),
        }
    }

    /// Copies a file into the store without notifying peers.
    pub fn add_file(&mut self, path: impl AsRef<Path>) -> io::Result<()> {
        let mut file = fs::File::open(&path)?;
        let hash = DhtAddr::buffer_hash(file)?;
        fs::copy(&path, &self.blob_dir.join(hash.to_string()))?;
        Ok(())
    }

    /// Get a file from the blob store by using the hash value provided
    pub fn cat(&self, file_hash: impl AsRef<str>) -> io::Result<File> {
        return File::open(&self.blob_dir.join(PathBuf::from(&file_hash.as_ref())));
    }

    /// Delete a file from the blob store with the file hash
    pub fn remove(&self, file_hash: impl AsRef<str>) -> io::Result<()> {
        fs::remove_file(&self.blob_dir.join(PathBuf::from(&file_hash.as_ref())))?;
        Ok(())
    }
}