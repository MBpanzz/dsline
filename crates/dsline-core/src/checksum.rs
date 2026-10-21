//! Frame checksum: CRC-32 (IEEE), for corruption detection only.
//!
//! This is not authentication and not a security boundary — it detects torn
//! or corrupted payloads (roadmap §6.4).
//!
//! The algorithm matters for throughput, not for correctness: this runs over
//! every payload byte on both the send and the receive path, so a serial
//! byte-at-a-time hash becomes the copy path's dominant per-byte cost. The
//! previous byte-at-a-time FNV-1a measured 4.19 µs for a 4 KiB payload —
//! ~63% of the whole 6.64 µs send and of the 6.55 µs receive (release build).
//! CRC-32 with carry-less-multiplication folding runs at multiple GB/s.
//!
//! Changing the function changes the stored checksum values, so it is a
//! protocol change: the frame version (`dsline_core::frame::FRAME_VERSION`)
//! and the on-region format word (`dsline_shm::REGION_FORMAT`) are both
//! bumped, and regions written by older builds are rejected at open.

/// CRC-32 (IEEE) of `bytes`.
pub fn checksum32(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

#[cfg(test)]
mod tests {
    use super::checksum32;

    #[test]
    fn checksum_is_stable() {
        // Standard CRC-32 check values.
        assert_eq!(checksum32(b""), 0);
        assert_eq!(checksum32(b"123456789"), 0xCBF4_3926);
        assert_eq!(checksum32(b"hello"), checksum32(b"hello"));
        assert_ne!(checksum32(b"hello"), checksum32(b"hellO"));
    }

    #[test]
    fn checksum_detects_single_bit_corruption() {
        let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        let clean = checksum32(&payload);
        for index in [0usize, 1, 2047, 4095] {
            let mut corrupted = payload.clone();
            corrupted[index] ^= 0x01;
            assert_ne!(checksum32(&corrupted), clean, "bit flip at {index}");
        }
    }
}
