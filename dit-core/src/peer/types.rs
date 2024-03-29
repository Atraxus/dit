use rand::Rng;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::io::{self, BufRead, BufReader, Read};
use std::str::FromStr;

pub use std::net::SocketAddr;

/// This address uniquely identifies peers and data stored on the distributed hash table.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct DhtAddr(pub [u8; Self::BYTE_LEN]);

impl DhtAddr {
    pub const BYTE_LEN: usize = 32;
    pub const STR_LEN: usize = 2 * Self::BYTE_LEN;
    pub const BITS: usize = 8 * Self::BYTE_LEN;

    /// Returns a `DhtAddr` with all bits set to zero.
    pub const fn zero() -> Self {
        Self([0; Self::BYTE_LEN])
    }

    /// Returns 2<sup>`exp`</sup>.
    ///
    /// # Panics
    ///
    /// Panics if `exp >= DhtAddr::BITS`.
    #[track_caller]
    pub const fn pow2(exp: usize) -> Self {
        assert!(exp < Self::BITS, "DhtAddr::pow2: exponent too large");
        let mut value = Self::zero();
        value.0[Self::BYTE_LEN - 1 - (exp / 8)] = 1 << (exp % 8);
        value
    }

    /// Returns a random `DhtAddr`.
    pub fn random() -> Self {
        Self(rand::thread_rng().gen())
    }

    /// Creates a `DhtAddr` by hashing an in-memory buffer.
    pub fn hash(data: impl AsRef<[u8]>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(data.as_ref());
        Self(hasher.finalize().into())
    }

    /// Creates a `DhtAddr` by hashing a stream of data.
    pub fn hash_reader(file: &mut impl Read) -> io::Result<Self> {
        const BUFFER_SIZE: usize = 512;
        let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);
        let mut hasher = Sha256::new();
        loop {
            let buffer = reader.fill_buf()?;
            hasher.update(buffer);
            if buffer.is_empty() {
                break;
            }
            let length = buffer.len();
            reader.consume(length);
        }
        Ok(Self(hasher.finalize().into()))
    }

    pub fn is_zero(&self) -> bool {
        self.0.iter().all(|&byte| byte == 0)
    }

    /// Returns log<sub>2</sub>(`self`), rounded down.
    ///
    /// # Panics
    ///
    /// Panics if `self == DhtAddr::zero()`.
    #[track_caller]
    pub fn log2(&self) -> usize {
        let (index, &byte) = self
            .0
            .iter()
            .enumerate()
            .find(|(_, &byte)| byte != 0)
            .expect("DhtAddr::log2: argument was zero");
        8 * (Self::BYTE_LEN - 1 - index) + usize::try_from(byte.ilog2()).unwrap()
    }

    pub fn wrapping_sub(mut self, other: Self) -> Self {
        let mut carry = false;
        for (result_byte, other_byte) in self.0.iter_mut().zip(other.0).rev() {
            let (result_1, overflow_1) = result_byte.overflowing_sub(other_byte);
            let (result_2, overflow_2) = result_1.overflowing_sub(carry.into());
            *result_byte = result_2;
            carry = overflow_1 || overflow_2;
        }
        self
    }

    pub fn wrapping_add(mut self, other: Self) -> Self {
        let mut carry = false;
        for (result_byte, other_byte) in self.0.iter_mut().zip(other.0).rev() {
            let (result_1, overflow_1) = result_byte.overflowing_add(other_byte);
            let (result_2, overflow_2) = result_1.overflowing_add(carry.into());
            *result_byte = result_2;
            carry = overflow_1 || overflow_2;
        }
        self
    }
}

impl fmt::Display for DhtAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for DhtAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DhtAddr({self})")
    }
}

impl FromStr for DhtAddr {
    type Err = ParseDhtAddrError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.len() != Self::STR_LEN {
            return Err(ParseDhtAddrError::InvalidLength(input.len()));
        }

        let mut output = [0; Self::BYTE_LEN];
        for (index, tuple) in input.as_bytes().chunks_exact(2).enumerate() {
            let high = match tuple[0] {
                c @ b'0'..=b'9' => c - b'0',
                c @ b'A'..=b'F' => c - (b'A' - 10),
                c @ b'a'..=b'f' => c - (b'a' - 10),
                _ => {
                    return Err(ParseDhtAddrError::InvalidChar(
                        input[index..].chars().next().unwrap(),
                    ))
                }
            };
            let low = match tuple[1] {
                c @ b'0'..=b'9' => c - b'0',
                c @ b'A'..=b'F' => c - (b'A' - 10),
                c @ b'a'..=b'f' => c - (b'a' - 10),
                _ => {
                    return Err(ParseDhtAddrError::InvalidChar(
                        input[index..].chars().next().unwrap(),
                    ))
                }
            };
            output[index] = (high << 4) | low
        }

        Ok(Self(output))
    }
}

impl Serialize for DhtAddr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for DhtAddr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            deserializer.deserialize_str(DhtAddrFromStrVisitor)
        } else {
            <[u8; Self::BYTE_LEN]>::deserialize(deserializer).map(DhtAddr)
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DhtAddrFromStrVisitor;

impl<'de> de::Visitor<'de> for DhtAddrFromStrVisitor {
    type Value = DhtAddr;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DhtAddr")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        v.parse().map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseDhtAddrError {
    InvalidLength(usize),
    InvalidChar(char),
}

impl fmt::Display for ParseDhtAddrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseDhtAddrError::InvalidLength(length) => write!(
                f,
                "invalid length for DhtAddr, expected {}, got {length}",
                DhtAddr::STR_LEN,
            ),
            ParseDhtAddrError::InvalidChar(ch) => write!(
                f,
                "invalid char for DhtAddr, expected hex digit, found {ch}",
            ),
        }
    }
}

impl Error for ParseDhtAddrError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DhtAndSocketAddr {
    pub dht_addr: DhtAddr,
    pub socket_addr: SocketAddr,
}

/// The table of successors of a peer.
///
/// finger(index) = successor(self_addr + 2<sup>index</sup>)
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Fingers {
    map: BTreeMap<usize, DhtAndSocketAddr>,
}

impl Fingers {
    /// Returns an empty finger table.
    pub const fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    /// Inserts or replaces a finger.
    ///
    /// If the slot has a closer finger, this method is a no-op and returns `None`.
    ///
    /// If the slot has a further finer, it is replaced and the old finger is returned.
    pub fn insert(
        &mut self,
        self_addr: DhtAddr,
        addrs: DhtAndSocketAddr,
    ) -> Option<DhtAndSocketAddr> {
        let index = Self::index_of(self_addr, addrs.dht_addr);
        let old = self.map.get(&index);
        if !old.is_some_and(|old| {
            old.dht_addr.wrapping_sub(self_addr) <= addrs.dht_addr.wrapping_sub(self_addr)
        }) {
            self.map.insert(index, addrs)
        } else {
            None
        }
    }

    /// Removes a finger.
    pub fn remove(&mut self, self_addr: DhtAddr, dst_addr: DhtAddr) -> Option<DhtAndSocketAddr> {
        let index = Self::index_of(self_addr, dst_addr);
        self.map.remove(&index)
    }

    /// Returns the finger that should be used for routing from `self_addr` to `dst_addr`.
    ///
    /// Ideally, this is the finger furthest from `self_addr` and below `dst_addr`.
    /// If there is no finger below `dst_addr`, then the finger closest to `self_addr` is returned.
    pub fn get_route(&self, self_addr: DhtAddr, dst_addr: DhtAddr) -> Option<&DhtAndSocketAddr> {
        let max_index = Self::index_of(self_addr, dst_addr);
        let distance_to_dst = dst_addr.wrapping_sub(self_addr);
        self.map
            .range(..=max_index)
            .rev()
            .find(|(_, addrs)| addrs.dht_addr.wrapping_sub(self_addr) <= distance_to_dst)
            .or_else(|| self.map.first_key_value())
            .map(|(_, addrs)| addrs)
    }

    /// Returns the finger index of `dst_addr` for a peer with `self_addr`.
    ///
    /// # Panics
    ///
    /// Panics if `self_addr == dst_addr`.
    fn index_of(self_addr: DhtAddr, dst_addr: DhtAddr) -> usize {
        dst_addr.wrapping_sub(self_addr).log2()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(s: &str) -> DhtAddr {
        s.parse().unwrap()
    }

    #[test]
    fn dht_addr_hash() {
        let addr = DhtAddr::hash("hello");
        assert_eq!(
            format!("{addr}"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
        );
    }

    #[test]
    fn dht_addr_hash_stream() {
        struct Reader(u8);
        impl Read for Reader {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.0 < 8 {
                    buf[0] = self.0;
                    self.0 += 1;
                    Ok(1)
                } else {
                    Ok(0)
                }
            }
        }

        assert_eq!(
            DhtAddr::hash_reader(&mut Reader(0)).unwrap(),
            DhtAddr::hash([0, 1, 2, 3, 4, 5, 6, 7]),
        );
    }

    #[test]
    fn dht_addr_parse() {
        let addr = DhtAddr([
            0x2c, 0xf2, 0x4d, 0xba, 0x5f, 0xb0, 0xa3, 0x0e, 0x26, 0xe8, 0x3b, 0x2a, 0xc5, 0xb9,
            0xe2, 0x9e, 0x1b, 0x16, 0x1e, 0x5c, 0x1f, 0xa7, 0x42, 0x5e, 0x73, 0x04, 0x33, 0x62,
            0x93, 0x8b, 0x98, 0x24,
        ]);
        assert_eq!(
            Ok(addr),
            "2CF24DBA5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".parse()
        );
    }

    #[test]
    fn dht_addr_add_sub() {
        let a = DhtAddr([
            0x2c, 0xf2, 0x4d, 0xba, 0x5f, 0xb0, 0xa3, 0x0e, 0x26, 0xe8, 0x3b, 0x2a, 0xc5, 0xb9,
            0xe2, 0x9e, 0x1b, 0x16, 0x1e, 0x5c, 0x1f, 0xa7, 0x42, 0x5e, 0x73, 0x04, 0x33, 0x62,
            0x93, 0x8b, 0x98, 0x24,
        ]);
        let b = DhtAddr([
            0x48, 0x6e, 0xa4, 0x62, 0x24, 0xd1, 0xbb, 0x4f, 0xb6, 0x80, 0xf3, 0x4f, 0x7c, 0x9a,
            0xd9, 0x6a, 0x8f, 0x24, 0xec, 0x88, 0xbe, 0x73, 0xea, 0x8e, 0x5a, 0x6c, 0x65, 0x26,
            0x0e, 0x9c, 0xb8, 0xa7,
        ]);
        let a_minus_b = DhtAddr([
            0xe4, 0x83, 0xa9, 0x58, 0x3a, 0xde, 0xe7, 0xbe, 0x70, 0x67, 0x47, 0xdb, 0x49, 0x1f,
            0x09, 0x33, 0x8b, 0xf1, 0x31, 0xd3, 0x61, 0x33, 0x57, 0xd0, 0x18, 0x97, 0xce, 0x3c,
            0x84, 0xee, 0xdf, 0x7d,
        ]);
        let b_minus_a = DhtAddr([
            0x1b, 0x7c, 0x56, 0xa7, 0xc5, 0x21, 0x18, 0x41, 0x8f, 0x98, 0xb8, 0x24, 0xb6, 0xe0,
            0xf6, 0xcc, 0x74, 0x0e, 0xce, 0x2c, 0x9e, 0xcc, 0xa8, 0x2f, 0xe7, 0x68, 0x31, 0xc3,
            0x7b, 0x11, 0x20, 0x83,
        ]);

        assert_eq!(a.wrapping_sub(a), DhtAddr([0; DhtAddr::BYTE_LEN]));
        assert_eq!(a.wrapping_sub(b), a_minus_b);
        assert_eq!(b.wrapping_sub(a), b_minus_a);

        assert_eq!(b_minus_a.wrapping_add(a), b);
        assert_eq!(a.wrapping_add(b_minus_a), b);
        assert_eq!(a_minus_b.wrapping_add(b), a);
        assert_eq!(b.wrapping_add(a_minus_b), a);
    }

    #[test]
    fn pow2() {
        assert_eq!(
            format!("{}", DhtAddr::pow2(0)),
            "0000000000000000000000000000000000000000000000000000000000000001",
        );

        assert_eq!(
            format!("{}", DhtAddr::pow2(255)),
            "8000000000000000000000000000000000000000000000000000000000000000",
        );
    }

    #[test]
    #[should_panic = "exponent too large"]
    fn pow2_panic() {
        let _ = DhtAddr::pow2(256);
    }

    #[test]
    fn log2() {
        assert_eq!(
            addr("0000000000000000000000000000000000000000000000000000000000000001").log2(),
            0,
        );

        assert_eq!(
            addr("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff").log2(),
            255,
        );
    }

    #[test]
    #[should_panic = "argument was zero"]
    fn log2_panic() {
        let _ = DhtAddr::zero().log2();
    }

    #[test]
    fn fingers_insert_get() {
        let mut fingers = Fingers::new();

        let self_addr = addr("8000000000000000000000000000000000000000000000000000000000000000");

        let addrs_close = DhtAndSocketAddr {
            dht_addr: addr("C000000000000000000000000000000000000000000000000000000000000000"),
            socket_addr: "0.0.0.0:1".parse().unwrap(),
        };
        assert_eq!(fingers.insert(self_addr, addrs_close), None);

        let addrs_far = DhtAndSocketAddr {
            dht_addr: addr("4000000000000000000000000000000000000000000000000000000000000000"),
            socket_addr: "0.0.0.0:2".parse().unwrap(),
        };
        assert_eq!(fingers.insert(self_addr, addrs_far), None);

        assert_eq!(
            fingers.get_route(
                self_addr,
                addr("4000000000000000000000000000000000000000000000000000000000000000"),
            ),
            Some(&addrs_far)
        );

        assert_eq!(
            fingers.get_route(
                self_addr,
                addr("4000000000000000000000000000000000000000000000000000000000000001"),
            ),
            Some(&addrs_far)
        );

        assert_eq!(
            fingers.get_route(
                self_addr,
                addr("3FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"),
            ),
            Some(&addrs_close)
        );

        let addrs_far_further = DhtAndSocketAddr {
            dht_addr: addr("4000000000000000000000000000000000000000000000000000000000000001"),
            socket_addr: "0.0.0.0:3".parse().unwrap(),
        };
        assert_eq!(fingers.insert(self_addr, addrs_far_further), None);

        assert_eq!(
            fingers.get_route(
                self_addr,
                addr("4000000000000000000000000000000000000000000000000000000000000001"),
            ),
            Some(&addrs_far)
        );

        let addrs_far_closer = DhtAndSocketAddr {
            dht_addr: addr("3FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"),
            socket_addr: "0.0.0.0:3".parse().unwrap(),
        };
        assert_eq!(fingers.insert(self_addr, addrs_far_closer), Some(addrs_far));

        assert_eq!(
            fingers.get_route(
                self_addr,
                addr("4000000000000000000000000000000000000000000000000000000000000000"),
            ),
            Some(&addrs_far_closer)
        );
    }
}
