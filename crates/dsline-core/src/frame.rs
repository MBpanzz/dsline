use crate::checksum::checksum32;
use crate::error::{ProtocolError, Result};

pub const FRAME_MAGIC: u32 = 0x4453_4c4e;
pub const FRAME_VERSION: u16 = 1;
pub const FRAME_HEADER_LEN: usize = 50;
pub const TLV_HEADER_LEN: usize = 6;
pub const FRAME_FLAG_CHUNKED: u16 = 0x0001;
pub const FRAME_FLAG_CHUNK_START: u16 = 0x0002;
pub const FRAME_FLAG_CHUNK_END: u16 = 0x0004;
pub const TLV_CHUNK_INDEX: u16 = 100;
pub const TLV_CHUNK_TOTAL: u16 = 101;
pub const TLV_CHUNK_MESSAGE_LEN: u16 = 102;
pub const CHUNK_METADATA_LEN: usize = (TLV_HEADER_LEN + std::mem::size_of::<u64>()) * 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum FrameKind {
    Bytes = 1,
    Ndarray = 2,
    Arrow = 3,
    Control = 4,
}

impl TryFrom<u16> for FrameKind {
    type Error = ProtocolError;

    fn try_from(value: u16) -> std::result::Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Bytes),
            2 => Ok(Self::Ndarray),
            3 => Ok(Self::Arrow),
            4 => Ok(Self::Control),
            other => Err(ProtocolError::InvalidFrameKind(other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    pub flags: u16,
    pub kind: FrameKind,
    pub header_len: u32,
    pub payload_len: u64,
    pub seq: u64,
    pub timestamp_ns: u64,
    pub schema_hash: u64,
    pub checksum: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    pub ty: u16,
    pub value: Vec<u8>,
}

impl Metadata {
    pub fn new(ty: u16, value: impl Into<Vec<u8>>) -> Self {
        Self {
            ty,
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub header: FrameHeader,
    pub metadata: Vec<Metadata>,
    pub payload: Vec<u8>,
}

pub fn encode_frame(
    kind: FrameKind,
    flags: u16,
    seq: u64,
    timestamp_ns: u64,
    schema_hash: u64,
    metadata: &[Metadata],
    payload: &[u8],
) -> Result<Vec<u8>> {
    let metadata_len = metadata.iter().try_fold(0usize, |total, item| {
        total
            .checked_add(TLV_HEADER_LEN)
            .and_then(|value| value.checked_add(item.value.len()))
            .ok_or(ProtocolError::HeaderLengthOverflow)
    })?;
    let header_len = FRAME_HEADER_LEN
        .checked_add(metadata_len)
        .ok_or(ProtocolError::HeaderLengthOverflow)?;
    let header_len_u32 =
        u32::try_from(header_len).map_err(|_| ProtocolError::HeaderLengthOverflow)?;

    let mut out = Vec::with_capacity(header_len + payload.len());
    push_u32(&mut out, FRAME_MAGIC);
    push_u16(&mut out, FRAME_VERSION);
    push_u16(&mut out, flags);
    push_u16(&mut out, kind as u16);
    push_u32(&mut out, header_len_u32);
    push_u64(&mut out, payload.len() as u64);
    push_u64(&mut out, seq);
    push_u64(&mut out, timestamp_ns);
    push_u64(&mut out, schema_hash);
    push_u32(&mut out, checksum32(payload));

    for item in metadata {
        push_u16(&mut out, item.ty);
        push_u32(&mut out, item.value.len() as u32);
        out.extend_from_slice(&item.value);
    }
    out.extend_from_slice(payload);
    Ok(out)
}

pub fn decode_frame(bytes: &[u8]) -> Result<Frame> {
    if bytes.len() < FRAME_HEADER_LEN {
        return Err(ProtocolError::FrameTooShort {
            len: bytes.len(),
            minimum: FRAME_HEADER_LEN,
        }
        .into());
    }

    let magic = read_u32(bytes, 0);
    if magic != FRAME_MAGIC {
        return Err(ProtocolError::InvalidMagic(magic).into());
    }

    let version = read_u16(bytes, 4);
    if version != FRAME_VERSION {
        return Err(ProtocolError::UnsupportedVersion(version).into());
    }

    let flags = read_u16(bytes, 6);
    let kind = FrameKind::try_from(read_u16(bytes, 8))?;
    let header_len = read_u32(bytes, 10) as usize;
    if header_len < FRAME_HEADER_LEN {
        return Err(ProtocolError::InvalidHeaderLength {
            header_len,
            minimum: FRAME_HEADER_LEN,
        }
        .into());
    }
    if bytes.len() < header_len {
        return Err(ProtocolError::FrameTooShort {
            len: bytes.len(),
            minimum: header_len,
        }
        .into());
    }

    let payload_len = read_u64(bytes, 14);
    let available_payload = bytes.len() - header_len;
    if payload_len > available_payload as u64 {
        return Err(ProtocolError::InvalidPayloadLength {
            declared: payload_len,
            available: available_payload,
        }
        .into());
    }

    let seq = read_u64(bytes, 22);
    let timestamp_ns = read_u64(bytes, 30);
    let schema_hash = read_u64(bytes, 38);
    let checksum = read_u32(bytes, 46);

    let metadata = decode_metadata(bytes, header_len)?;
    let payload_end = header_len + payload_len as usize;
    let payload = bytes[header_len..payload_end].to_vec();

    Ok(Frame {
        header: FrameHeader {
            flags,
            kind,
            header_len: header_len as u32,
            payload_len,
            seq,
            timestamp_ns,
            schema_hash,
            checksum,
        },
        metadata,
        payload,
    })
}

fn decode_metadata(bytes: &[u8], header_len: usize) -> Result<Vec<Metadata>> {
    let mut items = Vec::new();
    let mut offset = FRAME_HEADER_LEN;

    while offset < header_len {
        if header_len - offset < TLV_HEADER_LEN {
            return Err(ProtocolError::InvalidMetadataLength {
                offset,
                declared: header_len - offset,
            }
            .into());
        }

        let ty = read_u16(bytes, offset);
        let len = read_u32(bytes, offset + 2) as usize;
        let value_start = offset + TLV_HEADER_LEN;
        let Some(value_end) = value_start.checked_add(len) else {
            return Err(ProtocolError::InvalidMetadataLength {
                offset,
                declared: len,
            }
            .into());
        };
        if value_end > header_len {
            return Err(ProtocolError::InvalidMetadataLength {
                offset,
                declared: len,
            }
            .into());
        }

        items.push(Metadata {
            ty,
            value: bytes[value_start..value_end].to_vec(),
        });
        offset = value_end;
    }

    Ok(items)
}

fn push_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

#[cfg(test)]
mod tests {
    use super::{decode_frame, encode_frame, FrameKind, Metadata, FRAME_HEADER_LEN, FRAME_MAGIC};
    use crate::checksum::checksum32;
    use crate::error::{DslineError, ProtocolError};

    #[test]
    fn round_trips_bytes_frame_with_metadata() {
        let metadata = vec![
            Metadata::new(1, b"uint8".to_vec()),
            Metadata::new(7, b"tag".to_vec()),
        ];
        let encoded =
            encode_frame(FrameKind::Bytes, 3, 42, 1000, 99, &metadata, b"hello").expect("encode");

        let decoded = decode_frame(&encoded).expect("decode");

        assert_eq!(decoded.header.flags, 3);
        assert_eq!(decoded.header.kind, FrameKind::Bytes);
        assert_eq!(decoded.header.seq, 42);
        assert_eq!(decoded.header.timestamp_ns, 1000);
        assert_eq!(decoded.header.schema_hash, 99);
        assert_eq!(decoded.header.checksum, checksum32(b"hello"));
        assert_eq!(decoded.metadata, metadata);
        assert_eq!(decoded.payload, b"hello");
    }

    #[test]
    fn rejects_short_frame() {
        assert_eq!(
            decode_frame(&[0; 4]).expect_err("short"),
            DslineError::Protocol(ProtocolError::FrameTooShort {
                len: 4,
                minimum: FRAME_HEADER_LEN
            })
        );
    }

    #[test]
    fn rejects_invalid_magic() {
        let mut encoded =
            encode_frame(FrameKind::Bytes, 0, 0, 0, 0, &[], b"payload").expect("encode");
        encoded[0..4].copy_from_slice(&0u32.to_le_bytes());

        assert_eq!(
            decode_frame(&encoded).expect_err("bad magic"),
            DslineError::Protocol(ProtocolError::InvalidMagic(0))
        );
    }

    #[test]
    fn rejects_invalid_payload_length() {
        let mut encoded =
            encode_frame(FrameKind::Bytes, 0, 0, 0, 0, &[], b"payload").expect("encode");
        encoded.truncate(encoded.len() - 1);

        assert_eq!(
            decode_frame(&encoded).expect_err("bad payload length"),
            DslineError::Protocol(ProtocolError::InvalidPayloadLength {
                declared: 7,
                available: 6
            })
        );
    }

    #[test]
    fn rejects_truncated_metadata() {
        let metadata = vec![Metadata::new(1, b"abc".to_vec())];
        let mut encoded =
            encode_frame(FrameKind::Bytes, 0, 0, 0, 0, &metadata, b"payload").expect("encode");
        let shorter_header_len = (FRAME_HEADER_LEN + 2) as u32;
        encoded[10..14].copy_from_slice(&shorter_header_len.to_le_bytes());

        assert_eq!(
            decode_frame(&encoded).expect_err("bad metadata length"),
            DslineError::Protocol(ProtocolError::InvalidMetadataLength {
                offset: FRAME_HEADER_LEN,
                declared: 2
            })
        );
    }

    #[test]
    fn uses_documented_magic_constant() {
        assert_eq!(FRAME_MAGIC.to_be_bytes(), *b"DSLN");
    }
}
