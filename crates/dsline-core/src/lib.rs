//! Core protocol, error, and ring-buffer primitives for dsline.

pub mod checksum;
pub mod error;
pub mod frame;
pub mod spsc;

pub use error::{ChannelError, DslineError, ProtocolError, Result};
pub use frame::{
    decode_frame, encode_frame, Frame, FrameHeader, FrameKind, Metadata, FRAME_HEADER_LEN,
    FRAME_MAGIC, FRAME_VERSION, TLV_HEADER_LEN,
};
pub use spsc::{Backpressure, SpscBytesRing, SpscConfig};
