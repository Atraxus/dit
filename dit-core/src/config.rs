use crate::daemon::DaemonConfig;
use crate::peer::{DhtAddr, DhtAndSocketAddr, PeerConfig};
use crate::store::StoreConfig;
use std::error::Error;
use std::fs::File;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::Path;
use std::{fmt, io};

mod file {
    use crate::peer::DhtAddr;
    use serde::Deserialize;
    use std::net::SocketAddr;
    use std::path::PathBuf;

    #[derive(Debug, Default, Deserialize)]
    #[serde(deny_unknown_fields, default)]
    pub struct Root {
        pub peer: Peer,
        pub daemon: Daemon,
        pub store: Store,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Peer {
        pub listener: Option<SocketAddr>,
        pub dht_addr: Option<DhtAddr>,
        pub ttl: Option<u32>,
        pub max_packet_length: Option<u32>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Daemon {
        pub listener: Option<SocketAddr>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Store {
        pub dir: Option<PathBuf>,
    }
}

const DEFAULT_CONFIG_STRING: &str = include_str!("dit-config.toml");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalConfig {
    pub peer: PeerConfig,
    pub daemon: DaemonConfig,
    pub store: StoreConfig,
}

impl GlobalConfig {
    /// Creates a file and writes the default config to it.
    ///
    /// This function returns an error if the file exists and `overwrite` is not set.
    pub fn init(path: impl AsRef<Path>, overwrite: bool) -> io::Result<()> {
        Self::init_(path.as_ref(), overwrite)
    }

    fn init_(path: &Path, overwrite: bool) -> io::Result<()> {
        let mut file = File::options()
            .write(true)
            .create(true)
            .create_new(!overwrite)
            .open(path)?;
        file.write_all(DEFAULT_CONFIG_STRING.as_bytes())?;
        Ok(())
    }

    /// Reads the config from a file.
    pub fn read(path: impl AsRef<Path>) -> Result<Self, ReadConfigError> {
        Self::read_(path.as_ref())
    }

    fn read_(path: &Path) -> Result<Self, ReadConfigError> {
        let Some(config_dir) = path.parent() else {
            // `path` was "" or "/", assume that's not a valid file.
            // FIXME: use `ErrorKind::InvalidFilename`
            return Err(ReadConfigError::Io(io::ErrorKind::NotFound.into()));
        };

        let mut file = File::open(path)?;
        let mut string = String::new();
        file.read_to_string(&mut string)?;
        let config = Self::from_str(&string, config_dir)?;
        Ok(config)
    }

    fn from_str(s: &str, config_dir: &Path) -> Result<Self, InvalidConfig> {
        let parsed: file::Root = toml::from_str(s).map_err(InvalidConfig::ParseError)?;
        let Some(peer_socket_addr) = parsed.peer.listener else {
            return Err(InvalidConfig::MissingPeerListener);
        };

        let global_config = GlobalConfig {
            peer: PeerConfig {
                addrs: DhtAndSocketAddr {
                    dht_addr: parsed.peer.dht_addr.unwrap_or_else(DhtAddr::random),
                    socket_addr: peer_socket_addr,
                },
                ttl: parsed.peer.ttl.unwrap_or(16),
                max_packet_length: parsed.peer.max_packet_length.unwrap_or(1024),
            },
            daemon: DaemonConfig {
                socket_addr: parsed
                    .daemon
                    .listener
                    .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 7701))),
            },
            store: StoreConfig {
                dir: parsed.store.dir.unwrap_or_else(|| config_dir.join("store")),
            },
        };

        Ok(global_config)
    }
}

#[derive(Debug)]
pub enum InvalidConfig {
    ParseError(toml::de::Error),
    MissingPeerListener,
}

impl fmt::Display for InvalidConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InvalidConfig::ParseError(err) => write!(f, "{err}"),
            InvalidConfig::MissingPeerListener => {
                write!(f, "the field `peer.listener` is required")
            }
        }
    }
}

impl Error for InvalidConfig {}

#[derive(Debug)]
pub enum ReadConfigError {
    Io(io::Error),
    Invalid(InvalidConfig),
}

impl From<io::Error> for ReadConfigError {
    fn from(err: io::Error) -> ReadConfigError {
        ReadConfigError::Io(err)
    }
}

impl From<InvalidConfig> for ReadConfigError {
    fn from(err: InvalidConfig) -> ReadConfigError {
        ReadConfigError::Invalid(err)
    }
}

impl fmt::Display for ReadConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadConfigError::Io(err) => write!(f, "failed to read config: {err}"),
            ReadConfigError::Invalid(err) => write!(f, "invalid config: {err}"),
        }
    }
}

impl Error for ReadConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! multiline_str {
        ($(#[doc = $s:expr])+) => {
            concat!($($s, "\n", )+)
        };
        (no_trailing_newline #[doc = $first:expr] $(#[doc = $tail:expr])*) => {
            concat!($first, $("\n", $tail, )*)
        };
    }

    #[track_caller]
    fn parse_unwrap(input: &str) -> GlobalConfig {
        GlobalConfig::from_str(input, Path::new("$CONFIG_DIR")).unwrap()
    }

    #[track_caller]
    fn assert_error(input: &str, error: &str) {
        assert_eq!(
            GlobalConfig::from_str(input, Path::new("$CONFIG_DIR"))
                .unwrap_err()
                .to_string(),
            error
        );
    }

    #[test]
    fn empty() {
        assert_error("", "the field `peer.listener` is required")
    }

    #[test]
    fn unknown_field() {
        assert_error(
            multiline_str!(
                ///[peer]
                ///listener = "1.2.3.4:5"
                ///no = true
            ),
            multiline_str!(
                ///TOML parse error at line 3, column 1
                ///  |
                ///3 | no = true
                ///  | ^^
                ///unknown field `no`, expected one of `listener`, `dht_addr`, `ttl`, `max_packet_length`
            ),
        )
    }

    #[test]
    fn minimal_smoke() {
        let _ = parse_unwrap(multiline_str!(
            no_trailing_newline
            ///[peer]
            ///listener = "[::]:0"
        ));
    }

    #[test]
    fn default() {
        let mut default_config = parse_unwrap(DEFAULT_CONFIG_STRING);

        let mut chars = DEFAULT_CONFIG_STRING.chars().peekable();
        let mut uncommented = String::new();
        while let Some(ch) = chars.next() {
            if ch != '#' || chars.peek().copied() == Some(' ') {
                uncommented.push(ch);
            }
        }
        let uncommented_config = parse_unwrap(&uncommented);

        // The default is randomized, so we unrandomize it here.
        default_config.peer.addrs.dht_addr =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .parse()
                .unwrap();

        assert_eq!(default_config.store.dir, Path::new("$CONFIG_DIR/store"));
        default_config.store.dir = "/path/to/store".into();

        assert_eq!(default_config, uncommented_config);
    }

    #[test]
    fn init() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("dit-config.toml");
        GlobalConfig::init(&config_path, /* overwrite = */ false).unwrap();
        GlobalConfig::init(&config_path, /* overwrite = */ false).unwrap_err();
        GlobalConfig::init(&config_path, /* overwrite = */ true).unwrap();
        GlobalConfig::read(&config_path).unwrap();
    }
}
