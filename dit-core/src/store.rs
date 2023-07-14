use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreConfig {
    pub dir: PathBuf,
}
