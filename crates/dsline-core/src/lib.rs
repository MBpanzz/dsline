//! Core protocol, error, and ring-buffer primitives for dsline.

pub mod checksum;
pub mod error;
pub mod frame;
pub mod spsc;

pub use error::{ChannelError, DslineError, ProtocolError, Result};
pub use frame::{
    decode_frame, encode_frame, Frame, FrameHeader, FrameKind, Metadata, CHUNK_METADATA_LEN,
    FRAME_FLAG_CHUNKED, FRAME_FLAG_CHUNK_END, FRAME_FLAG_CHUNK_START, FRAME_HEADER_LEN,
    FRAME_MAGIC, FRAME_VERSION, TLV_CHUNK_INDEX, TLV_CHUNK_MESSAGE_LEN, TLV_CHUNK_TOTAL,
    TLV_HEADER_LEN,
};
pub use spsc::{Backpressure, SpscBytesRing, SpscConfig};
