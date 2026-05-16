//! Small checksum helper for the prototype frame path.
//!
//! This is for corruption detection only. It is not authentication.

const FNV_OFFSET: u32 = 0x811c_9dc5;
const FNV_PRIME: u32 = 0x0100_0193;

pub fn checksum32(bytes: &[u8]) -> u32 {
    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::checksum32;

    #[test]
    fn checksum_is_stable() {
        assert_eq!(checksum32(b""), 0x811c_9dc5);
        assert_eq!(checksum32(b"hello"), checksum32(b"hello"));
        assert_ne!(checksum32(b"hello"), checksum32(b"hellO"));
    }
}
