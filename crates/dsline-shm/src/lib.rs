//! Shared-memory backend.
//!
//! The 0.0.1 target is a Linux-first fixed-slot SPSC bytes backend. The public
//! API is intentionally conservative until ADR-001 is accepted.

use dsline_core::{
    checksum::checksum32, decode_frame, encode_frame, Backpressure, ChannelError, Frame, FrameKind,
    Metadata, Result, SpscConfig, CHUNK_METADATA_LEN, FRAME_FLAG_CHUNKED, FRAME_FLAG_CHUNK_END,
    FRAME_FLAG_CHUNK_START, FRAME_HEADER_LEN, TLV_CHUNK_INDEX, TLV_CHUNK_MESSAGE_LEN,
    TLV_CHUNK_TOTAL,
};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{fence, AtomicU64, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::Instant;

use memmap2::MmapMut;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SlotState {
    Free = 0,
    Writing = 1,
    Committed = 2,
    Pinned = 3,
    Corrupted = 4,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotSnapshot {
    pub state: SlotState,
    pub seq: u64,
    pub payload_len: usize,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedMessage {
    pub seq: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChunkInfo {
    index: usize,
    total: usize,
    message_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SendPlan {
    chunk_payload_size: usize,
    needed_slots: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageSizeLimits {
    pub slot_size: usize,
    pub chunk_metadata_size: usize,
    pub chunk_payload_size: usize,
    pub max_message_size: usize,
}

fn send_plan(config: &SpscConfig, len: usize) -> Result<SendPlan> {
    if len <= config.slot_size {
        return Ok(SendPlan {
            chunk_payload_size: config.slot_size,
            needed_slots: 1,
        });
    }

    let Some(chunk_payload_size) = config.slot_size.checked_sub(CHUNK_METADATA_LEN) else {
        return Err(ChannelError::MessageTooLarge {
            len,
            slot_size: max_message_size(config),
        }
        .into());
    };
    if chunk_payload_size == 0 {
        return Err(ChannelError::MessageTooLarge {
            len,
            slot_size: max_message_size(config),
        }
        .into());
    }

    let needed_slots = len
        .checked_add(chunk_payload_size - 1)
        .ok_or(ChannelError::InvalidConfig("message size overflow"))?
        / chunk_payload_size;
    let max_chunked_message_size = config
        .capacity
        .checked_mul(chunk_payload_size)
        .ok_or(ChannelError::InvalidConfig("message size overflow"))?;
    if needed_slots > config.capacity || len > max_chunked_message_size {
        return Err(ChannelError::MessageTooLarge {
            len,
            slot_size: max_message_size(config),
        }
        .into());
    }

    Ok(SendPlan {
        chunk_payload_size,
        needed_slots,
    })
}

fn max_message_size(config: &SpscConfig) -> usize {
    message_size_limits(config).max_message_size
}

fn message_size_limits(config: &SpscConfig) -> MessageSizeLimits {
    let chunk_payload_size = config
        .slot_size
        .checked_sub(CHUNK_METADATA_LEN)
        .unwrap_or(0);
    let max_message_size = config
        .capacity
        .checked_mul(chunk_payload_size)
        .map(|chunked_size| chunked_size.max(config.slot_size))
        .unwrap_or(config.slot_size);
    MessageSizeLimits {
        slot_size: config.slot_size,
        chunk_metadata_size: CHUNK_METADATA_LEN,
        chunk_payload_size,
        max_message_size,
    }
}

fn chunk_metadata(index: usize, total: usize, message_len: usize) -> Result<Vec<Metadata>> {
    Ok(vec![
        Metadata::new(TLV_CHUNK_INDEX, usize_to_u64_bytes(index)?),
        Metadata::new(TLV_CHUNK_TOTAL, usize_to_u64_bytes(total)?),
        Metadata::new(TLV_CHUNK_MESSAGE_LEN, usize_to_u64_bytes(message_len)?),
    ])
}

fn chunk_flags(index: usize, total: usize) -> u16 {
    let mut flags = FRAME_FLAG_CHUNKED;
    if index == 0 {
        flags |= FRAME_FLAG_CHUNK_START;
    }
    if index + 1 == total {
        flags |= FRAME_FLAG_CHUNK_END;
    }
    flags
}

fn usize_to_u64_bytes(value: usize) -> Result<Vec<u8>> {
    let value =
        u64::try_from(value).map_err(|_| ChannelError::InvalidConfig("chunk metadata overflow"))?;
    Ok(value.to_le_bytes().to_vec())
}

fn chunk_info(frame: &Frame) -> Result<ChunkInfo> {
    let index = metadata_usize(frame, TLV_CHUNK_INDEX)?;
    let total = metadata_usize(frame, TLV_CHUNK_TOTAL)?;
    let message_len = metadata_usize(frame, TLV_CHUNK_MESSAGE_LEN)?;
    if total == 0 || index >= total {
        return Err(ChannelError::CorruptedMessage.into());
    }
    Ok(ChunkInfo {
        index,
        total,
        message_len,
    })
}

fn metadata_usize(frame: &Frame, ty: u16) -> Result<usize> {
    let metadata = frame
        .metadata
        .iter()
        .find(|item| item.ty == ty)
        .ok_or(ChannelError::CorruptedMessage)?;
    if metadata.value.len() != std::mem::size_of::<u64>() {
        return Err(ChannelError::CorruptedMessage.into());
    }
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&metadata.value);
    usize::try_from(u64::from_le_bytes(raw))
        .map_err(|_| ChannelError::InvalidConfig("chunk metadata overflow").into())
}

fn validate_bytes_frame(frame: &Frame) -> Result<()> {
    if frame.header.kind != FrameKind::Bytes || checksum32(&frame.payload) != frame.header.checksum
    {
        return Err(ChannelError::CorruptedMessage.into());
    }
    Ok(())
}

fn is_chunked(frame: &Frame) -> bool {
    frame.header.flags & FRAME_FLAG_CHUNKED != 0
}

fn sequence_precedes(left: u64, right: u64) -> bool {
    left != right && right.wrapping_sub(left) < (1u64 << 63)
}

fn encode_message_frames(
    config: &SpscConfig,
    plan: SendPlan,
    seq: u64,
    bytes: &[u8],
) -> Result<Vec<Vec<u8>>> {
    if plan.needed_slots == 1 {
        return Ok(vec![encode_frame(
            FrameKind::Bytes,
            0,
            seq,
            0,
            0,
            &[],
            bytes,
        )?]);
    }

    let mut frames = Vec::with_capacity(plan.needed_slots);
    for (index, chunk) in bytes.chunks(plan.chunk_payload_size).enumerate() {
        let metadata = chunk_metadata(index, plan.needed_slots, bytes.len())?;
        frames.push(encode_frame(
            FrameKind::Bytes,
            chunk_flags(index, plan.needed_slots),
            seq,
            0,
            0,
            &metadata,
            chunk,
        )?);
    }
    if frames.len() != plan.needed_slots {
        return Err(ChannelError::InvalidConfig("chunk plan mismatch").into());
    }
    if frames
        .iter()
        .any(|frame| frame.len() > config.slot_size + FRAME_HEADER_LEN)
    {
        return Err(ChannelError::InvalidConfig("chunk frame exceeds slot size").into());
    }
    Ok(frames)
}

fn validate_chunk(frame: &Frame, seq: u64, expected: ChunkInfo) -> Result<()> {
    validate_bytes_frame(frame)?;
    if frame.header.seq != seq || !is_chunked(frame) {
        return Err(ChannelError::CorruptedMessage.into());
    }
    let info = chunk_info(frame)?;
    if info != expected {
        return Err(ChannelError::CorruptedMessage.into());
    }
    let has_start = frame.header.flags & FRAME_FLAG_CHUNK_START != 0;
    let has_end = frame.header.flags & FRAME_FLAG_CHUNK_END != 0;
    if has_start != (info.index == 0) || has_end != (info.index + 1 == info.total) {
        return Err(ChannelError::CorruptedMessage.into());
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FixedSlot {
    state: SlotState,
    seq: u64,
    payload_len: usize,
}

impl Default for FixedSlot {
    fn default() -> Self {
        Self {
            state: SlotState::Free,
            seq: 0,
            payload_len: 0,
        }
    }
}

pub trait FixedSlotStorage: std::fmt::Debug {
    fn capacity(&self) -> usize;
    fn slot_size(&self) -> usize;
    fn write_slot(&mut self, index: usize, bytes: &[u8]) -> Result<()>;
    fn read_slot(&self, index: usize, len: usize) -> Result<Vec<u8>>;
    fn clear_slot(&mut self, index: usize) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySlotStorage {
    capacity: usize,
    slot_size: usize,
    data: Vec<u8>,
}

impl MemorySlotStorage {
    pub fn new(capacity: usize, slot_size: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(ChannelError::InvalidConfig("capacity must be greater than zero").into());
        }
        if slot_size == 0 {
            return Err(ChannelError::InvalidConfig("slot_size must be greater than zero").into());
        }
        let total_len = capacity
            .checked_mul(slot_size)
            .ok_or(ChannelError::InvalidConfig("storage size overflow"))?;

        Ok(Self {
            capacity,
            slot_size,
            data: vec![0; total_len],
        })
    }

    fn offset(&self, index: usize) -> Result<usize> {
        if index >= self.capacity {
            return Err(ChannelError::InvalidConfig("slot index out of range").into());
        }

        index
            .checked_mul(self.slot_size)
            .ok_or(ChannelError::InvalidConfig("slot offset overflow").into())
    }
}

impl FixedSlotStorage for MemorySlotStorage {
    fn capacity(&self) -> usize {
        self.capacity
    }

    fn slot_size(&self) -> usize {
        self.slot_size
    }

    fn write_slot(&mut self, index: usize, bytes: &[u8]) -> Result<()> {
        if bytes.len() > self.slot_size {
            return Err(ChannelError::MessageTooLarge {
                len: bytes.len(),
                slot_size: self.slot_size,
            }
            .into());
        }

        let offset = self.offset(index)?;
        let end = offset + bytes.len();
        self.data[offset..end].copy_from_slice(bytes);
        Ok(())
    }

    fn read_slot(&self, index: usize, len: usize) -> Result<Vec<u8>> {
        if len > self.slot_size {
            return Err(ChannelError::MessageTooLarge {
                len,
                slot_size: self.slot_size,
            }
            .into());
        }

        let offset = self.offset(index)?;
        Ok(self.data[offset..offset + len].to_vec())
    }

    fn clear_slot(&mut self, index: usize) -> Result<()> {
        let offset = self.offset(index)?;
        self.data[offset..offset + self.slot_size].fill(0);
        Ok(())
    }
}

/// Byte-range access to persistent slot storage shared across processes.
///
/// The storage is a flat array of `capacity` slots, each `slot_size` bytes.
/// Slot metadata (state, seq, payload_len) lives in the storage itself so
/// that multiple processes can coordinate through the shared backing store.
pub trait SlotRegionIo: std::fmt::Debug + Sized {
    /// Create a new region, truncating any existing backing store.
    ///
    /// `total_len` is the full backing size (region header + slot area).
    fn create(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<Self>;
    /// Open an existing region, validating that the backing store is large
    /// enough for `total_len` bytes.
    fn open(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<Self>;

    fn capacity(&self) -> usize;
    fn slot_size(&self) -> usize;
    fn path(&self) -> &Path;

    /// Read `len` bytes at `offset`.
    fn read_at(&self, offset: usize, len: usize) -> Result<Vec<u8>>;
    /// Write `bytes` at `offset`.
    fn write_at(&self, offset: usize, bytes: &[u8]) -> Result<()>;
    /// Write `len` zero bytes at `offset`.
    fn clear_at(&self, offset: usize, len: usize) -> Result<()>;
    /// Ensure writes in `[offset, offset + len)` are visible to other
    /// processes before returning. No-op for backends whose writes are
    /// already ordered (file I/O with flush).
    fn flush_range(&self, _offset: usize, _len: usize) -> Result<()> {
        Ok(())
    }

    /// Load an 8-byte cursor value at `offset` with acquire semantics.
    ///
    /// Default: byte I/O through `read_at` with a `SeqCst` fence (file
    /// backends — cross-process ordering is provided by the OS file layer).
    fn load_u64(&self, offset: usize) -> Result<u64> {
        let bytes = self.read_at(offset, 8)?;
        fence(Ordering::SeqCst);
        let value = u64::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| ChannelError::StorageIo("cursor read size"))?,
        );
        Ok(value)
    }

    /// Store an 8-byte cursor value at `offset` with release semantics.
    fn store_u64(&self, offset: usize, value: u64) -> Result<()> {
        fence(Ordering::SeqCst);
        self.write_at(offset, &value.to_le_bytes())
    }

    /// Advance an 8-byte cursor at `offset` to `max(current, value)`.
    /// Used for the consumer-owned tail cursor, which both the consumer
    /// (consume) and the producer (DropOldest) advance.
    fn fetch_max_u64(&self, offset: usize, value: u64) -> Result<u64> {
        let current = self.load_u64(offset)?;
        if value > current {
            self.store_u64(offset, value)?;
            Ok(value)
        } else {
            Ok(current)
        }
    }
}

#[derive(Debug)]
pub struct FileSlotStorage {
    capacity: usize,
    slot_size: usize,
    path: PathBuf,
    file: Mutex<File>,
}

impl FileSlotStorage {
    pub fn create(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<Self> {
        if capacity == 0 {
            return Err(ChannelError::InvalidConfig("capacity must be greater than zero").into());
        }
        if slot_size == 0 {
            return Err(ChannelError::InvalidConfig("slot_size must be greater than zero").into());
        }
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|_| ChannelError::StorageIo("open file storage"))?;
        file.set_len(total_len as u64)
            .map_err(|_| ChannelError::StorageIo("resize file storage"))?;

        Ok(Self {
            capacity,
            slot_size,
            path,
            file: Mutex::new(file),
        })
    }

    pub fn open(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<Self> {
        if capacity == 0 {
            return Err(ChannelError::InvalidConfig("capacity must be greater than zero").into());
        }
        if slot_size == 0 {
            return Err(ChannelError::InvalidConfig("slot_size must be greater than zero").into());
        }
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|_| ChannelError::StorageIo("open file storage"))?;
        let actual_len = file
            .metadata()
            .map_err(|_| ChannelError::StorageIo("stat file storage"))?
            .len();
        if actual_len < total_len as u64 {
            return Err(
                ChannelError::InvalidConfig("file storage is smaller than expected").into(),
            );
        }

        Ok(Self {
            capacity,
            slot_size,
            path,
            file: Mutex::new(file),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn offset(&self, index: usize) -> Result<u64> {
        if index >= self.capacity {
            return Err(ChannelError::InvalidConfig("slot index out of range").into());
        }

        let offset = index
            .checked_mul(self.slot_size)
            .ok_or(ChannelError::InvalidConfig("slot offset overflow"))?;
        Ok(offset as u64)
    }

    fn validate_range(&self, offset: usize, len: usize) -> Result<()> {
        let total = self
            .capacity
            .checked_mul(self.slot_size)
            .ok_or(ChannelError::InvalidConfig("storage size overflow"))?;
        if offset > total || len > total - offset {
            return Err(ChannelError::InvalidConfig("file storage range out of bounds").into());
        }
        Ok(())
    }
}

impl SlotRegionIo for FileSlotStorage {
    fn create(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<Self> {
        Self::create(path, capacity, slot_size, total_len)
    }

    fn open(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<Self> {
        Self::open(path, capacity, slot_size, total_len)
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn slot_size(&self) -> usize {
        self.slot_size
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn read_at(&self, offset: usize, len: usize) -> Result<Vec<u8>> {
        self.validate_range(offset, len)?;
        let mut data = vec![0; len];
        let mut file = self
            .file
            .lock()
            .map_err(|_| ChannelError::StorageIo("file storage lock poisoned"))?;
        file.seek(SeekFrom::Start(offset as u64))
            .map_err(|_| ChannelError::StorageIo("seek file storage"))?;
        file.read_exact(&mut data)
            .map_err(|_| ChannelError::StorageIo("read file storage"))?;
        Ok(data)
    }

    fn write_at(&self, offset: usize, bytes: &[u8]) -> Result<()> {
        self.validate_range(offset, bytes.len())?;
        let mut file = self
            .file
            .lock()
            .map_err(|_| ChannelError::StorageIo("file storage lock poisoned"))?;
        file.seek(SeekFrom::Start(offset as u64))
            .map_err(|_| ChannelError::StorageIo("seek file storage"))?;
        file.write_all(bytes)
            .map_err(|_| ChannelError::StorageIo("write file storage"))?;
        file.flush()
            .map_err(|_| ChannelError::StorageIo("flush file storage"))?;
        Ok(())
    }

    fn clear_at(&self, offset: usize, len: usize) -> Result<()> {
        self.validate_range(offset, len)?;
        let zeros = vec![0; len];
        let mut file = self
            .file
            .lock()
            .map_err(|_| ChannelError::StorageIo("file storage lock poisoned"))?;
        file.seek(SeekFrom::Start(offset as u64))
            .map_err(|_| ChannelError::StorageIo("seek file storage"))?;
        file.write_all(&zeros)
            .map_err(|_| ChannelError::StorageIo("clear file storage"))?;
        file.flush()
            .map_err(|_| ChannelError::StorageIo("flush file storage"))?;
        Ok(())
    }
}

impl FixedSlotStorage for FileSlotStorage {
    fn capacity(&self) -> usize {
        self.capacity
    }

    fn slot_size(&self) -> usize {
        self.slot_size
    }

    fn write_slot(&mut self, index: usize, bytes: &[u8]) -> Result<()> {
        if bytes.len() > self.slot_size {
            return Err(ChannelError::MessageTooLarge {
                len: bytes.len(),
                slot_size: self.slot_size,
            }
            .into());
        }
        SlotRegionIo::write_at(self, self.offset(index)? as usize, bytes)
    }

    fn read_slot(&self, index: usize, len: usize) -> Result<Vec<u8>> {
        if len > self.slot_size {
            return Err(ChannelError::MessageTooLarge {
                len,
                slot_size: self.slot_size,
            }
            .into());
        }
        SlotRegionIo::read_at(self, self.offset(index)? as usize, len)
    }

    fn clear_slot(&mut self, index: usize) -> Result<()> {
        SlotRegionIo::clear_at(self, self.offset(index)? as usize, self.slot_size)
    }
}

/// A slot region backed by a memory-mapped file.
///
/// The mapping is shared across every process that maps the same file, which
/// makes this the OS shared-memory backend for channels.
#[derive(Debug)]
pub struct MmapSlotStorage {
    capacity: usize,
    slot_size: usize,
    total_len: usize,
    path: PathBuf,
    /// Kept alive so the mapping never outlives the file handle it derives
    /// from; resizing or truncating the file while mapped is undefined.
    _file: File,
    map: Mutex<MmapMut>,
}

impl MmapSlotStorage {
    pub fn create(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<Self> {
        Self::create_with_options(path, capacity, slot_size, total_len, false)
    }

    /// Create a new region only if the backing file does not exist yet.
    pub fn create_new(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<Self> {
        Self::create_with_options(path, capacity, slot_size, total_len, true)
    }

    fn create_with_options(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
        create_new: bool,
    ) -> Result<Self> {
        if capacity == 0 {
            return Err(ChannelError::InvalidConfig("capacity must be greater than zero").into());
        }
        if slot_size == 0 {
            return Err(ChannelError::InvalidConfig("slot_size must be greater than zero").into());
        }
        let path = path.as_ref().to_path_buf();
        let mut options = OpenOptions::new();
        options.read(true).write(true);
        if create_new {
            options.create_new(true);
        } else {
            options.create(true).truncate(true);
        }
        let file = options
            .open(&path)
            .map_err(|_| ChannelError::StorageIo("open mmap storage"))?;
        file.set_len(total_len as u64)
            .map_err(|_| ChannelError::StorageIo("resize mmap storage"))?;

        Self::map(file, path, capacity, slot_size, total_len)
    }

    pub fn open(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<Self> {
        if capacity == 0 {
            return Err(ChannelError::InvalidConfig("capacity must be greater than zero").into());
        }
        if slot_size == 0 {
            return Err(ChannelError::InvalidConfig("slot_size must be greater than zero").into());
        }
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|_| ChannelError::StorageIo("open mmap storage"))?;
        let actual_len = file
            .metadata()
            .map_err(|_| ChannelError::StorageIo("stat mmap storage"))?
            .len();
        if actual_len < total_len as u64 {
            return Err(
                ChannelError::InvalidConfig("file storage is smaller than expected").into(),
            );
        }

        Self::map(file, path, capacity, slot_size, total_len)
    }

    fn map(
        file: File,
        path: PathBuf,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<Self> {
        // SAFETY: the file handle is kept in `_file` for the lifetime of the
        // mapping and the file is never resized or truncated while mapped.
        let map = unsafe { MmapMut::map_mut(&file) }
            .map_err(|_| ChannelError::StorageIo("map mmap storage"))?;
        Ok(Self {
            capacity,
            slot_size,
            total_len,
            path,
            _file: file,
            map: Mutex::new(map),
        })
    }

    fn validate_range(&self, offset: usize, len: usize) -> Result<()> {
        if offset > self.total_len || len > self.total_len - offset {
            return Err(ChannelError::InvalidConfig("mmap storage range out of bounds").into());
        }
        Ok(())
    }
}

impl SlotRegionIo for MmapSlotStorage {
    fn create(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<Self> {
        Self::create(path, capacity, slot_size, total_len)
    }

    fn open(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<Self> {
        Self::open(path, capacity, slot_size, total_len)
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn slot_size(&self) -> usize {
        self.slot_size
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn read_at(&self, offset: usize, len: usize) -> Result<Vec<u8>> {
        self.validate_range(offset, len)?;
        let map = self
            .map
            .lock()
            .map_err(|_| ChannelError::StorageIo("mmap storage lock poisoned"))?;
        Ok(map[offset..offset + len].to_vec())
    }

    fn write_at(&self, offset: usize, bytes: &[u8]) -> Result<()> {
        self.validate_range(offset, bytes.len())?;
        let mut map = self
            .map
            .lock()
            .map_err(|_| ChannelError::StorageIo("mmap storage lock poisoned"))?;
        let end = offset + bytes.len();
        map[offset..end].copy_from_slice(bytes);
        Ok(())
    }

    fn load_u64(&self, offset: usize) -> Result<u64> {
        self.validate_range(offset, 8)?;
        let map = self
            .map
            .lock()
            .map_err(|_| ChannelError::StorageIo("mmap storage lock poisoned"))?;
        // SAFETY: `offset` is 8-aligned (region header cursor constants) and
        // within the mapping, which outlives the lock guard; the underlying
        // memory is shared with other processes, where the hardware provides
        // the atomicity of aligned 8-byte accesses.
        let ptr = unsafe { map.as_ptr().add(offset) } as *const AtomicU64;
        Ok(unsafe { (&*ptr).load(Ordering::Acquire) })
    }

    fn store_u64(&self, offset: usize, value: u64) -> Result<()> {
        self.validate_range(offset, 8)?;
        let mut map = self
            .map
            .lock()
            .map_err(|_| ChannelError::StorageIo("mmap storage lock poisoned"))?;
        // SAFETY: as in `load_u64`; a release store publishes the slot writes
        // that precede it to a consumer that acquire-loads the cursor.
        let ptr = unsafe { map.as_mut_ptr().add(offset) } as *mut AtomicU64;
        unsafe { (&*ptr).store(value, Ordering::Release) };
        Ok(())
    }

    fn fetch_max_u64(&self, offset: usize, value: u64) -> Result<u64> {
        self.validate_range(offset, 8)?;
        let mut map = self
            .map
            .lock()
            .map_err(|_| ChannelError::StorageIo("mmap storage lock poisoned"))?;
        // SAFETY: as in `load_u64`; the RMW advances the shared cursor
        // monotonically even when two roles (consumer consume, producer
        // DropOldest) advance it concurrently.
        let ptr = unsafe { map.as_mut_ptr().add(offset) } as *mut AtomicU64;
        Ok(unsafe { (&*ptr).fetch_max(value, Ordering::Release) })
    }

    fn clear_at(&self, offset: usize, len: usize) -> Result<()> {
        self.validate_range(offset, len)?;
        let mut map = self
            .map
            .lock()
            .map_err(|_| ChannelError::StorageIo("mmap storage lock poisoned"))?;
        map[offset..offset + len].fill(0);
        Ok(())
    }

    fn flush_range(&self, offset: usize, len: usize) -> Result<()> {
        let map = self
            .map
            .lock()
            .map_err(|_| ChannelError::StorageIo("mmap storage lock poisoned"))?;
        // FlushViewOfFile only (memmap2's flush_async_range): required for
        // writes to be visible to OTHER processes' mappings of the same file
        // on Windows (file-backed sections are not coherent across views
        // without a flush; paging-file-backed sections like
        // multiprocessing.shared_memory are). memmap2's `flush_range`
        // additionally calls FlushFileBuffers, a full disk sync that costs
        // ~1ms per call — never use it per message.
        map.flush_async_range(offset, len)
            .map_err(|_| ChannelError::StorageIo("flush mmap storage").into())
    }
}

#[derive(Debug)]
pub struct FixedSlotRegion<S: FixedSlotStorage = MemorySlotStorage> {
    storage: S,
    slots: Vec<FixedSlot>,
}

impl FixedSlotRegion<MemorySlotStorage> {
    pub fn new(capacity: usize, slot_size: usize) -> Result<Self> {
        Self::with_storage(MemorySlotStorage::new(capacity, slot_size)?)
    }
}

impl<S: FixedSlotStorage> FixedSlotRegion<S> {
    pub fn with_storage(storage: S) -> Result<Self> {
        let capacity = storage.capacity();
        if capacity == 0 {
            return Err(ChannelError::InvalidConfig("capacity must be greater than zero").into());
        }
        if storage.slot_size() == 0 {
            return Err(ChannelError::InvalidConfig("slot_size must be greater than zero").into());
        }
        Ok(Self {
            storage,
            slots: (0..capacity).map(|_| FixedSlot::default()).collect(),
        })
    }

    pub fn capacity(&self) -> usize {
        self.storage.capacity()
    }

    pub fn slot_size(&self) -> usize {
        self.storage.slot_size()
    }

    pub fn state(&self, index: usize) -> Result<SlotState> {
        Ok(self.slot(index)?.state)
    }

    pub fn write_committed(&mut self, index: usize, seq: u64, frame: &[u8]) -> Result<()> {
        if frame.len() > self.storage.slot_size() {
            return Err(ChannelError::MessageTooLarge {
                len: frame.len(),
                slot_size: self.storage.slot_size(),
            }
            .into());
        }

        if self.slot(index)?.state != SlotState::Free {
            return Err(ChannelError::BufferFull.into());
        }

        {
            let slot = self.slot_mut(index)?;
            slot.state = SlotState::Writing;
            slot.seq = seq;
            slot.payload_len = frame.len();
        }
        self.storage.write_slot(index, frame)?;
        let slot = self.slot_mut(index)?;
        slot.state = SlotState::Committed;
        Ok(())
    }

    pub fn read_committed(&mut self, index: usize) -> Result<SlotSnapshot> {
        let slot = self.slot(index)?;
        if slot.state != SlotState::Committed {
            return Err(ChannelError::BufferEmpty.into());
        }
        let seq = slot.seq;
        let payload_len = slot.payload_len;

        let data = self.storage.read_slot(index, payload_len)?;
        let slot = self.slot_mut(index)?;
        slot.state = SlotState::Pinned;
        Ok(SlotSnapshot {
            state: SlotState::Pinned,
            seq,
            payload_len,
            data,
        })
    }

    pub fn peek_committed(&self, index: usize) -> Result<SlotSnapshot> {
        let slot = self.slot(index)?;
        if slot.state != SlotState::Committed {
            return Err(ChannelError::BufferEmpty.into());
        }
        let seq = slot.seq;
        let payload_len = slot.payload_len;
        let data = self.storage.read_slot(index, payload_len)?;
        Ok(SlotSnapshot {
            state: SlotState::Committed,
            seq,
            payload_len,
            data,
        })
    }

    pub fn release_pinned(&mut self, index: usize) -> Result<()> {
        if self.slot(index)?.state != SlotState::Pinned {
            return Err(ChannelError::InvalidConfig("slot is not pinned").into());
        }

        self.release_slot(index)
    }

    pub fn release_committed(&mut self, index: usize) -> Result<()> {
        if self.slot(index)?.state != SlotState::Committed {
            return Err(ChannelError::InvalidConfig("slot is not committed").into());
        }

        self.release_slot(index)
    }

    fn release_slot(&mut self, index: usize) -> Result<()> {
        self.storage.clear_slot(index)?;
        let slot = self.slot_mut(index)?;
        slot.state = SlotState::Free;
        slot.seq = 0;
        slot.payload_len = 0;
        Ok(())
    }

    pub fn mark_corrupted(&mut self, index: usize) -> Result<()> {
        let slot = self.slot_mut(index)?;
        slot.state = SlotState::Corrupted;
        Ok(())
    }

    #[cfg(test)]
    fn corrupt_checksum_for_test(&mut self, index: usize) {
        let payload_len = self.slot(index).expect("slot exists").payload_len;
        let mut data = self
            .storage
            .read_slot(index, payload_len)
            .expect("slot data exists");
        let checksum_offset = 46;
        let checksum = u32::from_le_bytes([
            data[checksum_offset],
            data[checksum_offset + 1],
            data[checksum_offset + 2],
            data[checksum_offset + 3],
        ]);
        data[checksum_offset..checksum_offset + 4]
            .copy_from_slice(&checksum.wrapping_add(1).to_le_bytes());
        self.storage
            .write_slot(index, &data)
            .expect("write corrupted data");
    }

    #[cfg(test)]
    fn overwrite_frame_seq_for_test(&mut self, index: usize, seq: u64) -> Result<()> {
        let payload_len = self.slot(index)?.payload_len;
        let mut data = self.storage.read_slot(index, payload_len)?;
        data[22..30].copy_from_slice(&seq.to_le_bytes());
        self.storage.write_slot(index, &data)
    }

    fn slot(&self, index: usize) -> Result<&FixedSlot> {
        self.slots
            .get(index)
            .ok_or(ChannelError::InvalidConfig("slot index out of range").into())
    }

    fn slot_mut(&mut self, index: usize) -> Result<&mut FixedSlot> {
        self.slots
            .get_mut(index)
            .ok_or(ChannelError::InvalidConfig("slot index out of range").into())
    }
}

// ── Persistent slot region (shared-memory, cross-process) ──

/// Size of the per-slot header stored in persistent storage.
pub const PERSISTENT_SLOT_HEADER_LEN: usize = 13;

/// Size of the region header at the start of the backing store, holding the
/// SPSC cursors: `head` (producer-owned, monotonic slot count) at offset 0
/// and `tail` (consumer-owned) at offset 8.
pub const REGION_HEADER_LEN: usize = 16;

const CURSOR_HEAD_OFFSET: usize = 0;
const CURSOR_TAIL_OFFSET: usize = 8;

/// Resolve a channel name to a stable region file path in the runtime
/// directory. The same name always maps to the same path, in every process,
/// so `ShmChannel`-style channels can connect by name.
pub fn shm_region_path(name: &str) -> Result<PathBuf> {
    let dir = shm_runtime_dir()?;
    let digest = checksum32(name.as_bytes());
    Ok(dir.join(format!("dsline-{digest:08x}.bin")))
}

fn shm_runtime_dir() -> Result<PathBuf> {
    let dir = shm_runtime_base()?.join("dsline").join("shm");
    std::fs::create_dir_all(&dir)
        .map_err(|_| ChannelError::StorageIo("create shm runtime directory"))?;
    Ok(dir)
}

#[cfg(windows)]
fn shm_runtime_base() -> Result<PathBuf> {
    let base = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("TEMP").map(PathBuf::from));
    base.ok_or_else(|| ChannelError::InvalidConfig("LOCALAPPDATA is not set").into())
}

#[cfg(not(windows))]
fn shm_runtime_base() -> Result<PathBuf> {
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .or_else(|| std::env::var_os("TMPDIR"))
        .map(PathBuf::from);
    Ok(base.unwrap_or_else(std::env::temp_dir))
}

/// A slot region that stores metadata (state, seq, length) in the storage
/// layer alongside frame data, enabling cross-process access.
///
/// Slot layout in storage:
/// ```text
/// [state: u8] [seq: u64 LE] [payload_len: u32 LE] [frame bytes…]
/// ```
/// Total per-slot region size for a channel config (header + frame space).
fn persistent_region_slot_size(config: &SpscConfig) -> Result<usize> {
    config
        .slot_size
        .checked_add(FRAME_HEADER_LEN)
        .and_then(|v| v.checked_add(PERSISTENT_SLOT_HEADER_LEN))
        .ok_or(ChannelError::InvalidConfig("slot_size is too large").into())
}

/// A slot region that stores metadata (state, seq, length) in the storage
/// layer alongside frame data, enabling cross-process access.
///
/// Slot layout in storage:
/// ```text
/// [state: u8] [seq: u64 LE] [payload_len: u32 LE] [frame bytes…]
/// ```
#[derive(Debug)]
pub struct PersistentSlotRegion<S: SlotRegionIo = MmapSlotStorage> {
    storage: S,
    /// Usable frame space after subtracting the header.
    frame_capacity: usize,
}

impl<S: SlotRegionIo> PersistentSlotRegion<S> {
    /// Create a new persistent region.
    ///
    /// `slot_size` is the total per-slot size (header + max frame).
    pub fn create(path: impl AsRef<Path>, capacity: usize, slot_size: usize) -> Result<Self> {
        if slot_size <= PERSISTENT_SLOT_HEADER_LEN {
            return Err(ChannelError::InvalidConfig(
                "slot_size must be greater than persistent header size",
            )
            .into());
        }
        let total_len = Self::total_len(capacity, slot_size)?;
        let storage = S::create(path, capacity, slot_size, total_len)?;
        Ok(Self {
            frame_capacity: slot_size - PERSISTENT_SLOT_HEADER_LEN,
            storage,
        })
    }

    fn total_len(capacity: usize, slot_size: usize) -> Result<usize> {
        capacity
            .checked_mul(slot_size)
            .and_then(|v| v.checked_add(REGION_HEADER_LEN))
            .ok_or(ChannelError::InvalidConfig("storage size overflow").into())
    }

    /// Open an existing persistent region.
    pub fn open(path: impl AsRef<Path>, capacity: usize, slot_size: usize) -> Result<Self> {
        if slot_size <= PERSISTENT_SLOT_HEADER_LEN {
            return Err(ChannelError::InvalidConfig(
                "slot_size must be greater than persistent header size",
            )
            .into());
        }
        let total_len = Self::total_len(capacity, slot_size)?;
        let storage = S::open(path, capacity, slot_size, total_len)?;
        Ok(Self {
            frame_capacity: slot_size - PERSISTENT_SLOT_HEADER_LEN,
            storage,
        })
    }

    /// Wrap an already-created storage instance.
    pub fn from_storage(storage: S) -> Result<Self> {
        if storage.slot_size() <= PERSISTENT_SLOT_HEADER_LEN {
            return Err(ChannelError::InvalidConfig(
                "slot_size must be greater than persistent header size",
            )
            .into());
        }
        Ok(Self {
            frame_capacity: storage.slot_size() - PERSISTENT_SLOT_HEADER_LEN,
            storage,
        })
    }

    pub fn capacity(&self) -> usize {
        self.storage.capacity()
    }

    pub fn slot_size(&self) -> usize {
        self.storage.slot_size()
    }

    pub fn frame_capacity(&self) -> usize {
        self.frame_capacity
    }

    pub fn path(&self) -> &Path {
        self.storage.path()
    }

    /// Read the header of a slot from storage. Returns `(state, seq, payload_len)`.
    fn read_header(&self, index: usize) -> Result<(SlotState, u64, usize)> {
        let raw = self
            .storage
            .read_at(self.slot_offset(index), PERSISTENT_SLOT_HEADER_LEN)?;
        let state = match raw[0] {
            0 => SlotState::Free,
            1 => SlotState::Writing,
            2 => SlotState::Committed,
            3 => SlotState::Pinned,
            4 => SlotState::Corrupted,
            _ => SlotState::Corrupted,
        };
        let seq = u64::from_le_bytes(raw[1..9].try_into().expect("seq slice is 8 bytes"));
        let payload_len =
            u32::from_le_bytes(raw[9..13].try_into().expect("payload_len slice is 4 bytes"))
                as usize;
        Ok((state, seq, payload_len))
    }

    fn write_header(
        &self,
        index: usize,
        state: SlotState,
        seq: u64,
        payload_len: usize,
    ) -> Result<()> {
        let mut header = vec![0u8; PERSISTENT_SLOT_HEADER_LEN];
        header[0] = state as u8;
        header[1..9].copy_from_slice(&seq.to_le_bytes());
        header[9..13].copy_from_slice(&(payload_len as u32).to_le_bytes());
        self.storage.write_at(self.slot_offset(index), &header)
    }

    /// Write frame data to slot payload area (after header).
    fn write_payload(&self, index: usize, frame: &[u8]) -> Result<()> {
        if frame.len() > self.frame_capacity {
            return Err(ChannelError::MessageTooLarge {
                len: frame.len(),
                slot_size: self.frame_capacity,
            }
            .into());
        }
        self.storage
            .write_at(self.slot_offset(index) + PERSISTENT_SLOT_HEADER_LEN, frame)
    }

    fn read_payload(&self, index: usize, len: usize) -> Result<Vec<u8>> {
        if len > self.frame_capacity {
            return Err(ChannelError::MessageTooLarge {
                len,
                slot_size: self.frame_capacity,
            }
            .into());
        }
        self.storage
            .read_at(self.slot_offset(index) + PERSISTENT_SLOT_HEADER_LEN, len)
    }

    fn clear_payload(&self, index: usize) -> Result<()> {
        self.storage.clear_at(
            self.slot_offset(index) + PERSISTENT_SLOT_HEADER_LEN,
            self.frame_capacity,
        )
    }

    fn slot_offset(&self, index: usize) -> usize {
        REGION_HEADER_LEN + index * self.storage.slot_size()
    }

    /// Load the producer-owned head cursor (acquire).
    pub fn load_head(&self) -> Result<u64> {
        self.storage.load_u64(CURSOR_HEAD_OFFSET)
    }

    /// Publish the head cursor after all slot writes (release).
    pub fn store_head(&self, head: u64) -> Result<()> {
        self.storage.store_u64(CURSOR_HEAD_OFFSET, head)
    }

    /// Load the consumer-owned tail cursor (acquire).
    pub fn load_tail(&self) -> Result<u64> {
        self.storage.load_u64(CURSOR_TAIL_OFFSET)
    }

    /// Advance the tail cursor monotonically (release). Called by the
    /// consumer on consume and by the producer for DropOldest.
    pub fn fetch_max_tail(&self, tail: u64) -> Result<u64> {
        self.storage.fetch_max_u64(CURSOR_TAIL_OFFSET, tail)
    }

    // ── public state-transition API ──

    /// Query the current state of a slot from storage.
    pub fn state(&self, index: usize) -> Result<SlotState> {
        Ok(self.read_header(index)?.0)
    }

    /// Write a committed frame to a free slot.
    ///
    /// Protocol: write WRITING header → write payload → write COMMITTED
    /// header. Cross-process visibility is ordered with release/acquire
    /// fences and a `flush_range` (FlushViewOfFile) after the COMMITTED
    /// header — on Windows, file-backed mappings are not coherent across
    /// views without a flush. No per-message disk sync (FlushFileBuffers);
    /// system-crash durability is out of scope.
    pub fn write_committed(&self, index: usize, seq: u64, frame: &[u8]) -> Result<()> {
        self.write_committed_no_flush(index, seq, frame)?;
        // Make the whole slot visible to other processes' mappings.
        self.storage.flush_range(
            self.slot_offset(index),
            PERSISTENT_SLOT_HEADER_LEN + frame.len(),
        )
    }

    /// Same as `write_committed` but defers the cross-process flush. The
    /// caller must publish the writes with a `flush_range` (usually one
    /// batch flush covering the slots and the head cursor) before another
    /// process is allowed to observe them.
    fn write_committed_no_flush(&self, index: usize, seq: u64, frame: &[u8]) -> Result<()> {
        let (state, _, _) = self.read_header(index)?;
        if state != SlotState::Free {
            return Err(ChannelError::BufferFull.into());
        }
        // Pairs with the Release fence in `release_slot`: the slot is truly
        // free (payload cleared) before we write into it.
        fence(Ordering::Acquire);

        // Phase 1: mark WRITING
        self.write_header(index, SlotState::Writing, seq, frame.len())?;

        // Phase 2: write payload
        self.write_payload(index, frame)?;

        // Phase 3: commit — the Release fence orders the payload writes
        // before the COMMITTED header, so a reader that observes COMMITTED
        // (and pairs with an Acquire fence) never sees stale payload bytes.
        fence(Ordering::Release);
        self.write_header(index, SlotState::Committed, seq, frame.len())
    }

    /// Read a committed frame, transitioning the slot to PINNED.
    pub fn read_committed(&self, index: usize) -> Result<SlotSnapshot> {
        let (state, seq, payload_len) = self.read_header(index)?;
        if state != SlotState::Committed {
            return Err(ChannelError::BufferEmpty.into());
        }
        // Pairs with the Release fence in `write_committed`: the payload
        // published before COMMITTED is visible to this read.
        fence(Ordering::Acquire);

        let data = self.read_payload(index, payload_len)?;

        // Mark as pinned (reader has the data).
        self.write_header(index, SlotState::Pinned, seq, payload_len)?;

        Ok(SlotSnapshot {
            state: SlotState::Pinned,
            seq,
            payload_len,
            data,
        })
    }

    pub fn peek_committed(&self, index: usize) -> Result<SlotSnapshot> {
        let (state, seq, payload_len) = self.read_header(index)?;
        if state != SlotState::Committed {
            return Err(ChannelError::BufferEmpty.into());
        }
        let data = self.read_payload(index, payload_len)?;
        Ok(SlotSnapshot {
            state: SlotState::Committed,
            seq,
            payload_len,
            data,
        })
    }

    /// Release a pinned slot back to FREE.
    pub fn release_pinned(&self, index: usize) -> Result<()> {
        let (state, _, _) = self.read_header(index)?;
        if state != SlotState::Pinned {
            return Err(ChannelError::InvalidConfig("slot is not pinned").into());
        }
        self.release_slot(index)
    }

    pub fn release_committed(&self, index: usize) -> Result<()> {
        let (state, _, _) = self.read_header(index)?;
        if state != SlotState::Committed {
            return Err(ChannelError::InvalidConfig("slot is not committed").into());
        }
        self.release_slot(index)
    }

    /// As `release_pinned`, deferring the cross-process flush (see
    /// `release_slot_no_flush`). Channel-private: the channel publishes the
    /// released slot together with the tail cursor in one batch flush.
    fn release_pinned_no_flush(&self, index: usize) -> Result<()> {
        let (state, _, _) = self.read_header(index)?;
        if state != SlotState::Pinned {
            return Err(ChannelError::InvalidConfig("slot is not pinned").into());
        }
        self.release_slot_no_flush(index)
    }

    /// As `release_committed`, deferring the cross-process flush.
    fn release_committed_no_flush(&self, index: usize) -> Result<()> {
        let (state, _, _) = self.read_header(index)?;
        if state != SlotState::Committed {
            return Err(ChannelError::InvalidConfig("slot is not committed").into());
        }
        self.release_slot_no_flush(index)
    }

    fn release_slot(&self, index: usize) -> Result<()> {
        self.release_slot_no_flush(index)?;
        // Make the FREE header visible to the producer's mapping (Windows).
        self.storage
            .flush_range(self.slot_offset(index), PERSISTENT_SLOT_HEADER_LEN)
    }

    fn release_slot_no_flush(&self, index: usize) -> Result<()> {
        self.clear_payload(index)?;
        // Order the payload clear before the FREE header: a producer that
        // observes FREE (and pairs with an Acquire fence) never overwrites a
        // slot whose payload is still visible to the consumer.
        fence(Ordering::Release);
        self.write_header(index, SlotState::Free, 0, 0)
    }

    /// Publish the cursor words and one slot to other processes' mappings.
    ///
    /// Uses a single flush over `[0, end]` while the range stays small, and
    /// two page-local flushes (slot first, then the cursor words in the
    /// first page) for larger offsets: FlushViewOfFile scans every page in
    /// the range, so one large-range call costs more than two small ones
    /// once the slot sits far from the first page. The slot-first order
    /// keeps the reader's view consistent: it may see the slot released
    /// with a stale cursor (it retries), never a new cursor with a stale
    /// slot.
    fn flush_release(&self, index: usize) -> Result<()> {
        const MAX_SINGLE_FLUSH: usize = 8 * 4096;
        let off = self.slot_offset(index);
        let end = off + PERSISTENT_SLOT_HEADER_LEN;
        if end <= MAX_SINGLE_FLUSH {
            self.storage.flush_range(0, end)
        } else {
            self.storage.flush_range(off, PERSISTENT_SLOT_HEADER_LEN)?;
            self.storage.flush_range(0, REGION_HEADER_LEN)
        }
    }

    /// Publish a committed batch of slot frames plus the cursor words.
    ///
    /// Small batches (region offsets up to `MAX_SINGLE_FLUSH`) are
    /// published with one flush over `[0, last-slot end]`, which covers
    /// every slot of the batch and the cursor together. Larger batches fall
    /// back to one page-local flush per slot followed by the cursor flush,
    /// so the cost stays proportional to the batch, not the region.
    fn publish_batch(&self, frames: &[Vec<u8>], first_index: usize) -> Result<()> {
        const MAX_SINGLE_FLUSH: usize = 8 * 4096;
        let last_offset = if frames.is_empty() {
            self.slot_offset(first_index)
        } else {
            let last_index = (first_index + frames.len() - 1) % self.storage.capacity();
            self.slot_offset(last_index)
                + PERSISTENT_SLOT_HEADER_LEN
                + frames.last().map(Vec::len).unwrap_or(0)
        };
        if last_offset <= MAX_SINGLE_FLUSH {
            self.storage.flush_range(0, last_offset)
        } else {
            for (k, frame) in frames.iter().enumerate() {
                let index = (first_index + k) % self.storage.capacity();
                self.storage.flush_range(
                    self.slot_offset(index),
                    PERSISTENT_SLOT_HEADER_LEN + frame.len(),
                )?;
            }
            self.storage.flush_range(0, REGION_HEADER_LEN)
        }
    }

    /// Mark a slot as corrupted.
    pub fn mark_corrupted(&self, index: usize) -> Result<()> {
        let (_, seq, payload_len) = self.read_header(index)?;
        self.write_header(index, SlotState::Corrupted, seq, payload_len)?;
        Ok(())
    }

    #[cfg(test)]
    fn corrupt_checksum_for_test(&self, index: usize) -> Result<()> {
        let (_, _, payload_len) = self.read_header(index)?;
        let mut data = self.read_payload(index, payload_len)?;
        let checksum_offset = 46;
        let checksum = u32::from_le_bytes([
            data[checksum_offset],
            data[checksum_offset + 1],
            data[checksum_offset + 2],
            data[checksum_offset + 3],
        ]);
        data[checksum_offset..checksum_offset + 4]
            .copy_from_slice(&checksum.wrapping_add(1).to_le_bytes());
        self.write_payload(index, &data)
    }

    #[cfg(test)]
    fn overwrite_frame_seq_for_test(&self, index: usize, seq: u64) -> Result<()> {
        let (_, _, payload_len) = self.read_header(index)?;
        let mut data = self.read_payload(index, payload_len)?;
        data[22..30].copy_from_slice(&seq.to_le_bytes());
        self.write_payload(index, &data)
    }
}

/// A channel backed by a persistent slot region.
///
/// This channel can be opened by multiple processes that share the same
/// backing store (a file, or an OS shared-memory mapping). One process acts
/// as producer, another as consumer.
///
/// Ring position comes from two shared monotonic slot counts in the region
/// header: `head` (the number of slots the producer has fully committed,
/// published after each message with a release store and a batch flush) and
/// `tail` (the number of slots the consumer — or a `DropOldest` producer —
/// has released back to FREE). `refresh_cursors` reloads both counts in O(1),
/// so the per-operation cost no longer depends on the ring capacity; the
/// previous design re-scanned all slot headers on every operation to
/// re-derive the ring state.
///
/// The slot state machine (FREE → WRITING → COMMITTED → PINNED → FREE) is
/// retained for write/read safety: the producer never writes into a non-FREE
/// slot and the consumer only reads COMMITTED slots. The full slot-header
/// scan now runs once per open (recovery): it rebuilds the producer's next
/// message sequence, the consumer's expected sequence, and the producer's
/// write position when a crashed writer left an incomplete batch whose slots
/// the cursors cannot represent. Such leftovers are counted in the queue
/// depth (`recovery_bias`) and surfaced to the consumer as an empty ring
/// rather than a hang.
#[derive(Debug)]
pub struct PersistentShmChannel<S: SlotRegionIo = MmapSlotStorage> {
    region: PersistentSlotRegion<S>,
    config: SpscConfig,
    /// Producer-owned next write index, recovered at open and advanced
    /// locally. Never overwritten by a refresh: only the producer moves it.
    head: usize,
    /// Next read index, re-derived from the shared tail count at every
    /// refresh (a DropOldest producer also advances the shared tail).
    tail: usize,
    /// Queue depth: `(head_cursor - tail_cursor) + recovery_bias`, clamped
    /// to capacity. Mirrors the occupied count of the old full scan.
    len: usize,
    /// Monotonic slot counts mirrored from the shared region header.
    head_cursor: u64,
    tail_cursor: u64,
    /// Crash-leftover occupancy: slots occupied at open that the shared
    /// cursors do not count (a producer that died mid-write left WRITING
    /// slots, and the shared head never advanced past them). Kept in the
    /// depth so the send guard never over-commits a physically full ring.
    recovery_bias: usize,
    next_seq: u64,
    expected_recv_seq: u64,
    last_received_seq: Option<u64>,
    closed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PersistentSlotHeader {
    index: usize,
    state: SlotState,
    seq: u64,
}

impl<S: SlotRegionIo> PersistentShmChannel<S> {
    /// Create a new persistent channel, initialising the backing store.
    pub fn create(path: impl AsRef<Path>, config: SpscConfig) -> Result<Self> {
        config.validate()?;
        let region_slot_size = persistent_region_slot_size(&config)?;
        let region = PersistentSlotRegion::<S>::create(path, config.capacity, region_slot_size)?;
        Self::new_initialized(region, config)
    }

    /// Open an existing persistent channel.
    ///
    /// The caller is responsible for knowing whether it is the producer or
    /// consumer. This method scans the slot headers once to recover the
    /// sequence state (a fresh process must rebuild the producer's next
    /// sequence and the consumer's expected sequence from storage); ring
    /// position thereafter comes from the shared cursors.
    pub fn open(path: impl AsRef<Path>, config: SpscConfig) -> Result<Self> {
        config.validate()?;
        let region_slot_size = persistent_region_slot_size(&config)?;
        let region = PersistentSlotRegion::<S>::open(path, config.capacity, region_slot_size)?;

        let mut channel = Self {
            region,
            config,
            head: 0,
            tail: 0,
            len: 0,
            head_cursor: 0,
            tail_cursor: 0,
            recovery_bias: 0,
            next_seq: 0,
            expected_recv_seq: 0,
            last_received_seq: None,
            closed: false,
        };
        channel.recover_from_storage()?;
        Ok(channel)
    }

    fn new_initialized(region: PersistentSlotRegion<S>, config: SpscConfig) -> Result<Self> {
        let channel = Self {
            region,
            config,
            head: 0,
            tail: 0,
            len: 0,
            head_cursor: 0,
            tail_cursor: 0,
            recovery_bias: 0,
            next_seq: 0,
            expected_recv_seq: 0,
            last_received_seq: None,
            closed: false,
        };
        // Initialise all slots as FREE.
        for i in 0..channel.config.capacity {
            channel.region.write_header(i, SlotState::Free, 0, 0)?;
        }
        Ok(channel)
    }

    pub fn send(&mut self, bytes: &[u8]) -> Result<()> {
        if self.closed {
            return Err(ChannelError::Closed.into());
        }
        self.refresh_cursors()?;
        let plan = send_plan(&self.config, bytes.len())?;
        if !self.prepare_send_slots(plan.needed_slots)? {
            return Ok(());
        }

        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        let frames = encode_message_frames(&self.config, plan, seq, bytes)?;
        let first_index = self.head;
        for frame in &frames {
            self.region
                .write_committed_no_flush(self.head, seq, frame)?;
            self.head = (self.head + 1) % self.config.capacity;
            self.len += 1;
        }
        // Publish the batch: the release store of the head cursor orders
        // the payload and COMMITTED writes before it, and one flush over
        // [0, last-slot end] publishes the whole batch and the cursor
        // together (page-local flushes take over for larger offsets).
        self.head_cursor += frames.len() as u64;
        self.region.store_head(self.head_cursor)?;
        self.region.publish_batch(&frames, first_index)?;
        Ok(())
    }

    pub fn recv(&mut self) -> Result<Vec<u8>> {
        Ok(self.recv_with_seq()?.payload)
    }

    pub fn recv_with_seq(&mut self) -> Result<ReceivedMessage> {
        self.refresh_cursors()?;
        if self.closed && self.len == 0 {
            return Err(ChannelError::Closed.into());
        }

        // Gate: only a fully COMMITTED slot is readable. The producer
        // publishes the batch and the head cursor with one flush, so in a
        // healthy run this polls for at most the flush window; a WRITING
        // slot that stays put past the grace period is a crash leftover
        // (see verify C7), reported as an empty ring rather than hanging.
        if !self.poll_tail_committed()? {
            return Err(ChannelError::BufferEmpty.into());
        }

        let snapshot = self.region.read_committed(self.tail)?;

        if snapshot.seq != self.expected_recv_seq {
            return Err(ChannelError::SequenceMismatch {
                expected: self.expected_recv_seq,
                actual: snapshot.seq,
            }
            .into());
        }

        let decoded = decode_frame(&snapshot.data);
        let frame = match decoded {
            Ok(frame) => frame,
            Err(err) => {
                self.advance_tail_after_read()?;
                self.expected_recv_seq = self.expected_recv_seq.wrapping_add(1);
                return Err(err);
            }
        };
        if let Err(err) = validate_bytes_frame(&frame) {
            self.advance_tail_after_read()?;
            self.expected_recv_seq = self.expected_recv_seq.wrapping_add(1);
            return Err(err);
        }

        if snapshot.seq != frame.header.seq {
            let actual = frame.header.seq;
            self.advance_tail_after_read()?;
            self.expected_recv_seq = self.expected_recv_seq.wrapping_add(1);
            return Err(ChannelError::SequenceMismatch {
                expected: snapshot.seq,
                actual,
            }
            .into());
        }

        let seq = frame.header.seq;
        let payload = if is_chunked(&frame) {
            self.recv_chunked_message(snapshot.seq, frame)?
        } else {
            self.advance_tail_after_read()?;
            frame.payload
        };
        self.expected_recv_seq = self.expected_recv_seq.wrapping_add(1);
        self.last_received_seq = Some(seq);
        Ok(ReceivedMessage { seq, payload })
    }

    pub fn close(&mut self) {
        self.closed = true;
    }

    pub fn refresh(&mut self) -> Result<()> {
        self.refresh_cursors()
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn capacity(&self) -> usize {
        self.config.capacity
    }

    pub fn payload_slot_size(&self) -> usize {
        self.config.slot_size
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_seq
    }

    pub fn expected_recv_sequence(&self) -> u64 {
        self.expected_recv_seq
    }

    pub fn last_received_sequence(&self) -> Option<u64> {
        self.last_received_seq
    }

    pub fn message_size_limits(&self) -> MessageSizeLimits {
        message_size_limits(&self.config)
    }

    fn prepare_send_slots(&mut self, needed_slots: usize) -> Result<bool> {
        if needed_slots > self.config.capacity {
            return Err(ChannelError::MessageTooLarge {
                len: needed_slots,
                slot_size: self.config.capacity,
            }
            .into());
        }
        if self.config.capacity - self.len >= needed_slots {
            return Ok(true);
        }

        match self.config.backpressure {
            Backpressure::Raise => Err(ChannelError::BufferFull.into()),
            Backpressure::Block => {
                self.wait_for_space(needed_slots)?;
                Ok(true)
            }
            Backpressure::DropNewest => Ok(false),
            Backpressure::DropOldest => {
                while self.config.capacity - self.len < needed_slots {
                    self.drop_oldest_message()?;
                }
                Ok(true)
            }
        }
    }

    fn wait_for_space(&mut self, needed_slots: usize) -> Result<()> {
        if self.config.capacity - self.len >= needed_slots {
            return Ok(());
        }
        let Some(timeout) = self.config.timeout else {
            return Err(ChannelError::BufferFull.into());
        };
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            self.refresh_cursors()?;
            if self.config.capacity - self.len >= needed_slots {
                return Ok(());
            }
            thread::yield_now();
        }
        Err(ChannelError::BufferFull.into())
    }

    /// O(1) refresh: reload the shared head/tail cursors and re-derive the
    /// queue depth and the consumer's read position. `head` (the producer's
    /// write position) is intentionally not touched — only the producer
    /// moves it, and on open it was recovered zombie-aware.
    fn refresh_cursors(&mut self) -> Result<()> {
        let head_c = self.region.load_head()?;
        let tail_c = self.region.load_tail()?;
        // Both cursors must live in the same <2^63 window (the sequence
        // comparison rule). head < tail means the counts have diverged —
        // inconsistent region state, refuse rather than wrap into garbage.
        let dist = head_c.wrapping_sub(tail_c);
        if dist >= (1u64 << 63) {
            return Err(ChannelError::InvalidConfig("ring cursors are inconsistent").into());
        }
        self.head_cursor = head_c;
        self.tail_cursor = tail_c;
        self.tail = (tail_c % self.config.capacity as u64) as usize;
        self.len = (dist.min(self.config.capacity as u64) as usize + self.recovery_bias)
            .min(self.config.capacity);
        Ok(())
    }

    /// One-time recovery scan at open. The shared cursors carry the ring
    /// position; the scan rebuilds the pieces the cursors cannot represent:
    /// the producer's next message sequence (max occupied seq + 1), the
    /// consumer's expected sequence (the seq of the oldest readable slot),
    /// and the producer's write position (first FREE slot after the oldest
    /// readable slot — this also steps over any WRITING slot left by a
    /// crashed producer). `recovery_bias` keeps crash leftovers in the queue
    /// depth so the send guard never over-commits a physically full ring.
    fn recover_from_storage(&mut self) -> Result<()> {
        let occupied = self.occupied_slot_headers()?;
        let head_c = self.region.load_head()?;
        let tail_c = self.region.load_tail()?;
        let dist = head_c.wrapping_sub(tail_c);
        let cursor_len = dist.min(self.config.capacity as u64) as usize;

        self.head_cursor = head_c;
        self.tail_cursor = tail_c;
        let tail_index = (tail_c % self.config.capacity as u64) as usize;
        self.tail = tail_index;
        self.head = self.infer_head_from_tail(tail_index)?;
        // The scan sees every occupied slot; the cursors only see fully
        // committed batches. The difference is crash leftovers.
        self.recovery_bias = occupied.len().saturating_sub(cursor_len);
        self.len = (cursor_len + self.recovery_bias).min(self.config.capacity);

        // Producer sequence: continue after the newest occupied slot.
        for slot in &occupied {
            let candidate = slot.seq.wrapping_add(1);
            if sequence_precedes(self.next_seq, candidate) {
                self.next_seq = candidate;
            }
        }
        // Consumer sequence: the first message to read is the oldest
        // committed/pinned slot; with nothing readable yet the sequence
        // restarts at 0 (the current-region convention).
        let readable = occupied.iter().any(|s| s.state != SlotState::Writing);
        self.expected_recv_seq = if readable {
            self.region.read_header(self.tail)?.1
        } else {
            0
        };
        Ok(())
    }

    fn occupied_slot_headers(&self) -> Result<Vec<PersistentSlotHeader>> {
        let mut occupied = Vec::new();
        for index in 0..self.config.capacity {
            let (state, seq, _) = self.region.read_header(index)?;
            // WRITING slots count as occupied for depth/guard purposes (a
            // producer must not over-commit a physically full ring), but they
            // are never readable.
            if matches!(
                state,
                SlotState::Committed | SlotState::Pinned | SlotState::Writing
            ) {
                occupied.push(PersistentSlotHeader { index, state, seq });
            }
        }
        Ok(occupied)
    }

    /// First FREE slot stepping forward from `tail` — the producer's next
    /// write position. Walks past WRITING slots (crash leftovers) too, so a
    /// recovered producer writes after them instead of into them.
    fn infer_head_from_tail(&self, tail: usize) -> Result<usize> {
        let mut head = tail;
        for _ in 0..self.config.capacity {
            if matches!(self.region.read_header(head)?.0, SlotState::Free) {
                return Ok(head);
            }
            head = (head + 1) % self.config.capacity;
            if head == tail {
                return Ok(head);
            }
        }
        Ok(head)
    }

    fn tail_is_committed(&self) -> Result<bool> {
        Ok(self.region.read_header(self.tail)?.0 == SlotState::Committed)
    }

    /// Bounded poll for the COMMITTED state at the tail. Returns `false`
    /// when the slot is still not committed after the grace period — a
    /// crash leftover (WRITING) or a release that was never republished
    /// (FREE) — which the caller reports as an empty ring. The bound keeps
    /// the consumer from hanging on a dead writer while absorbing the
    /// producer's batch-flush window.
    fn poll_tail_committed(&mut self) -> Result<bool> {
        const POLL_LIMIT: u32 = 256;
        for _ in 0..POLL_LIMIT {
            let (state, _, _) = self.region.read_header(self.tail)?;
            if state == SlotState::Committed {
                return Ok(true);
            }
            if self.closed {
                return Err(ChannelError::Closed.into());
            }
            thread::yield_now();
        }
        Ok(false)
    }

    fn advance_tail_after_read(&mut self) -> Result<()> {
        let index = self.tail;
        self.region.release_pinned_no_flush(index)?;
        self.tail = (self.tail + 1) % self.config.capacity;
        // Publish the FREE header and the advanced tail cursor (two
        // page-local flushes; the cursor lives in the first page).
        self.tail_cursor = self.tail_cursor.wrapping_add(1);
        self.region.fetch_max_tail(self.tail_cursor)?;
        // Publish the FREE header and the advanced tail cursor.
        self.region.flush_release(index)?;
        // `len` is re-derived from the cursors at the start of every
        // operation, so it can undercount while a producer is mid-write;
        // never go below zero (the next refresh recomputes the true depth).
        self.len = self.len.saturating_sub(1);
        Ok(())
    }

    fn recv_chunked_message(&mut self, seq: u64, first_frame: Frame) -> Result<Vec<u8>> {
        let first_info = match chunk_info(&first_frame) {
            Ok(info) => info,
            Err(err) => {
                // The first chunk is pinned by the caller; release it so the
                // tail can advance on a retry.
                self.region.release_pinned(self.tail)?;
                return Err(err);
            }
        };
        if first_info.index != 0 {
            self.region.release_pinned(self.tail)?;
            return Err(ChannelError::CorruptedMessage.into());
        }
        let mut payload = Vec::with_capacity(first_info.message_len);
        payload.extend_from_slice(&first_frame.payload);
        self.advance_tail_after_read()?;

        for index in 1..first_info.total {
            // The batch flush publishes all chunks and the head cursor
            // together, so when chunk 0 was visible the remaining chunks are
            // COMMITTED as well; the bounded poll is a crash-leftover
            // backstop.
            if !self.poll_tail_committed()? {
                return Err(ChannelError::BufferEmpty.into());
            }
            let snapshot = self.region.read_committed(self.tail)?;
            if snapshot.seq != seq {
                self.region.release_pinned(self.tail)?;
                return Err(ChannelError::SequenceMismatch {
                    expected: seq,
                    actual: snapshot.seq,
                }
                .into());
            }
            let frame = match decode_frame(&snapshot.data) {
                Ok(frame) => frame,
                Err(err) => {
                    self.region.release_pinned(self.tail)?;
                    return Err(err);
                }
            };
            if let Err(err) = validate_chunk(
                &frame,
                seq,
                ChunkInfo {
                    index,
                    total: first_info.total,
                    message_len: first_info.message_len,
                },
            ) {
                self.region.release_pinned(self.tail)?;
                return Err(err);
            }
            payload.extend_from_slice(&frame.payload);
            self.advance_tail_after_read()?;
        }

        if payload.len() != first_info.message_len {
            return Err(ChannelError::CorruptedMessage.into());
        }
        Ok(payload)
    }

    fn drop_oldest_message(&mut self) -> Result<()> {
        if self.len == 0 {
            return Ok(());
        }
        let snapshot = self.region.peek_committed(self.tail)?;
        let frame = decode_frame(&snapshot.data)?;
        let slots_to_drop = if is_chunked(&frame) {
            let info = chunk_info(&frame)?;
            if info.index != 0 {
                return Err(ChannelError::CorruptedMessage.into());
            }
            info.total
        } else {
            1
        };

        for _ in 0..slots_to_drop {
            let index = self.tail;
            self.region.release_committed_no_flush(index)?;
            self.tail = (self.tail + 1) % self.config.capacity;
            self.tail_cursor = self.tail_cursor.wrapping_add(1);
            self.region.fetch_max_tail(self.tail_cursor)?;
            self.region.flush_release(index)?;
            self.len = self.len.saturating_sub(1);
        }
        self.expected_recv_seq = self.expected_recv_seq.wrapping_add(1);
        Ok(())
    }
}
impl PersistentShmChannel<MmapSlotStorage> {
    /// Create the region if it does not exist yet, otherwise open it and
    /// recover the current state. Used for named shared-memory channels
    /// where the first caller initialises the region.
    pub fn create_or_open(path: impl AsRef<Path>, config: SpscConfig) -> Result<Self> {
        config.validate()?;
        let region_slot_size = persistent_region_slot_size(&config)?;

        if let Ok(storage) = MmapSlotStorage::create_new(
            &path,
            config.capacity,
            region_slot_size,
            PersistentSlotRegion::<MmapSlotStorage>::total_len(config.capacity, region_slot_size)?,
        ) {
            // Freshly created: initialise all slots as FREE.
            let region = PersistentSlotRegion::from_storage(storage)?;
            return Self::new_initialized(region, config);
        }

        // The region already exists: open and recover.
        Self::open(path, config)
    }
}

#[derive(Debug)]
pub struct ShmSpscChannel<S: FixedSlotStorage = MemorySlotStorage> {
    config: SpscConfig,
    region: FixedSlotRegion<S>,
    head: usize,
    tail: usize,
    len: usize,
    next_seq: u64,
    expected_recv_seq: u64,
    last_received_seq: Option<u64>,
    closed: bool,
}

impl ShmSpscChannel<MemorySlotStorage> {
    pub fn new(config: SpscConfig) -> Result<Self> {
        config.validate()?;
        let region_slot_size = config
            .slot_size
            .checked_add(FRAME_HEADER_LEN)
            .ok_or(ChannelError::InvalidConfig("slot_size is too large"))?;
        let capacity = config.capacity;
        let storage = MemorySlotStorage::new(capacity, region_slot_size)?;
        Self::with_storage(config, storage)
    }
}

impl<S: FixedSlotStorage> ShmSpscChannel<S> {
    pub fn with_storage(config: SpscConfig, storage: S) -> Result<Self> {
        config.validate()?;
        let minimum_region_slot_size = config
            .slot_size
            .checked_add(FRAME_HEADER_LEN)
            .ok_or(ChannelError::InvalidConfig("slot_size is too large"))?;
        if storage.capacity() != config.capacity {
            return Err(ChannelError::InvalidConfig("storage capacity mismatch").into());
        }
        if storage.slot_size() < minimum_region_slot_size {
            return Err(ChannelError::InvalidConfig("storage slot_size is too small").into());
        }
        Ok(Self {
            region: FixedSlotRegion::with_storage(storage)?,
            config,
            head: 0,
            tail: 0,
            len: 0,
            next_seq: 0,
            expected_recv_seq: 0,
            last_received_seq: None,
            closed: false,
        })
    }

    pub fn send(&mut self, bytes: &[u8]) -> Result<()> {
        if self.closed {
            return Err(ChannelError::Closed.into());
        }
        let plan = send_plan(&self.config, bytes.len())?;
        if !self.prepare_send_slots(plan.needed_slots)? {
            return Ok(());
        }

        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        let frames = encode_message_frames(&self.config, plan, seq, bytes)?;
        for frame in frames {
            self.region.write_committed(self.head, seq, &frame)?;
            self.head = (self.head + 1) % self.config.capacity;
            self.len += 1;
        }
        Ok(())
    }

    pub fn recv(&mut self) -> Result<Vec<u8>> {
        Ok(self.recv_with_seq()?.payload)
    }

    pub fn recv_with_seq(&mut self) -> Result<ReceivedMessage> {
        if self.closed && self.len == 0 {
            return Err(ChannelError::Closed.into());
        }
        if self.len == 0 {
            return Err(ChannelError::BufferEmpty.into());
        }

        let snapshot = self.region.read_committed(self.tail)?;

        if snapshot.seq != self.expected_recv_seq {
            return Err(ChannelError::SequenceMismatch {
                expected: self.expected_recv_seq,
                actual: snapshot.seq,
            }
            .into());
        }

        let decoded = decode_frame(&snapshot.data);
        let frame = match decoded {
            Ok(frame) => frame,
            Err(err) => {
                self.advance_tail_after_read()?;
                self.expected_recv_seq = self.expected_recv_seq.wrapping_add(1);
                return Err(err);
            }
        };
        if let Err(err) = validate_bytes_frame(&frame) {
            self.advance_tail_after_read()?;
            self.expected_recv_seq = self.expected_recv_seq.wrapping_add(1);
            return Err(err);
        }

        if snapshot.seq != frame.header.seq {
            let actual = frame.header.seq;
            self.advance_tail_after_read()?;
            self.expected_recv_seq = self.expected_recv_seq.wrapping_add(1);
            return Err(ChannelError::SequenceMismatch {
                expected: snapshot.seq,
                actual,
            }
            .into());
        }

        let seq = frame.header.seq;
        let payload = if is_chunked(&frame) {
            self.recv_chunked_message(snapshot.seq, frame)?
        } else {
            self.advance_tail_after_read()?;
            frame.payload
        };
        self.expected_recv_seq = self.expected_recv_seq.wrapping_add(1);
        self.last_received_seq = Some(seq);

        Ok(ReceivedMessage { seq, payload })
    }

    pub fn close(&mut self) {
        self.closed = true;
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn capacity(&self) -> usize {
        self.config.capacity
    }

    pub fn payload_slot_size(&self) -> usize {
        self.config.slot_size
    }

    pub fn region_slot_size(&self) -> usize {
        self.region.slot_size()
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_seq
    }

    pub fn expected_recv_sequence(&self) -> u64 {
        self.expected_recv_seq
    }

    pub fn last_received_sequence(&self) -> Option<u64> {
        self.last_received_seq
    }

    pub fn message_size_limits(&self) -> MessageSizeLimits {
        message_size_limits(&self.config)
    }

    fn prepare_send_slots(&mut self, needed_slots: usize) -> Result<bool> {
        if needed_slots > self.config.capacity {
            return Err(ChannelError::MessageTooLarge {
                len: needed_slots,
                slot_size: self.config.capacity,
            }
            .into());
        }
        if self.config.capacity - self.len >= needed_slots {
            return Ok(true);
        }

        match self.config.backpressure {
            Backpressure::Raise => Err(ChannelError::BufferFull.into()),
            Backpressure::Block => {
                self.wait_for_space(needed_slots)?;
                Ok(true)
            }
            Backpressure::DropNewest => Ok(false),
            Backpressure::DropOldest => {
                while self.config.capacity - self.len < needed_slots {
                    self.drop_oldest_message()?;
                }
                Ok(true)
            }
        }
    }

    fn wait_for_space(&self, needed_slots: usize) -> Result<()> {
        if self.config.capacity - self.len >= needed_slots {
            return Ok(());
        }
        let Some(timeout) = self.config.timeout else {
            return Err(ChannelError::BufferFull.into());
        };
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.config.capacity - self.len >= needed_slots {
                return Ok(());
            }
            thread::yield_now();
        }
        Err(ChannelError::BufferFull.into())
    }

    fn advance_tail_after_read(&mut self) -> Result<()> {
        self.region.release_pinned(self.tail)?;
        self.tail = (self.tail + 1) % self.config.capacity;
        self.len -= 1;
        Ok(())
    }

    fn recv_chunked_message(&mut self, seq: u64, first_frame: Frame) -> Result<Vec<u8>> {
        let first_info = match chunk_info(&first_frame) {
            Ok(info) => info,
            Err(err) => {
                // The first chunk is pinned by the caller; release it so the
                // tail can advance on a retry.
                self.region.release_pinned(self.tail)?;
                return Err(err);
            }
        };
        if first_info.index != 0 {
            self.region.release_pinned(self.tail)?;
            return Err(ChannelError::CorruptedMessage.into());
        }
        let mut payload = Vec::with_capacity(first_info.message_len);
        payload.extend_from_slice(&first_frame.payload);
        self.advance_tail_after_read()?;

        for index in 1..first_info.total {
            let snapshot = self.region.read_committed(self.tail)?;
            if snapshot.seq != seq {
                self.region.release_pinned(self.tail)?;
                return Err(ChannelError::SequenceMismatch {
                    expected: seq,
                    actual: snapshot.seq,
                }
                .into());
            }
            let frame = match decode_frame(&snapshot.data) {
                Ok(frame) => frame,
                Err(err) => {
                    self.region.release_pinned(self.tail)?;
                    return Err(err);
                }
            };
            if let Err(err) = validate_chunk(
                &frame,
                seq,
                ChunkInfo {
                    index,
                    total: first_info.total,
                    message_len: first_info.message_len,
                },
            ) {
                self.region.release_pinned(self.tail)?;
                return Err(err);
            }
            payload.extend_from_slice(&frame.payload);
            self.advance_tail_after_read()?;
        }

        if payload.len() != first_info.message_len {
            return Err(ChannelError::CorruptedMessage.into());
        }
        Ok(payload)
    }

    fn drop_oldest_message(&mut self) -> Result<()> {
        if self.len == 0 {
            return Ok(());
        }
        let snapshot = self.region.peek_committed(self.tail)?;
        let frame = decode_frame(&snapshot.data)?;
        let slots_to_drop = if is_chunked(&frame) {
            let info = chunk_info(&frame)?;
            if info.index != 0 {
                return Err(ChannelError::CorruptedMessage.into());
            }
            info.total
        } else {
            1
        };

        for _ in 0..slots_to_drop {
            self.region.release_committed(self.tail)?;
            self.tail = (self.tail + 1) % self.config.capacity;
            self.len -= 1;
        }
        self.expected_recv_seq = self.expected_recv_seq.wrapping_add(1);
        Ok(())
    }

    #[cfg(test)]
    fn corrupt_tail_checksum_for_test(&mut self) {
        self.region.corrupt_checksum_for_test(self.tail);
    }

    #[cfg(test)]
    fn corrupt_tail_frame_seq_for_test(&mut self, seq: u64) {
        self.region
            .overwrite_frame_seq_for_test(self.tail, seq)
            .expect("tail slot exists");
    }

    #[cfg(test)]
    fn set_next_sequence_for_test(&mut self, seq: u64) {
        self.next_seq = seq;
    }

    #[cfg(test)]
    fn set_expected_recv_sequence_for_test(&mut self, seq: u64) {
        self.expected_recv_seq = seq;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        shm_region_path, FileSlotStorage, FixedSlotRegion, FixedSlotStorage, MemorySlotStorage,
        MmapSlotStorage, PersistentShmChannel, ShmSpscChannel, SlotRegionIo, SlotState,
    };
    use dsline_core::error::{ChannelError, DslineError};
    use dsline_core::{
        decode_frame, encode_frame, Backpressure, FrameKind, SpscConfig, CHUNK_METADATA_LEN,
        FRAME_HEADER_LEN,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::time::Duration;

    fn test_file_path(name: &str) -> PathBuf {
        let mut path = std::env::current_dir().expect("current dir");
        path.push("target");
        path.push("test-storage");
        fs::create_dir_all(&path).expect("create test storage dir");
        path.push(format!("{name}-{}.bin", std::process::id()));
        if path.exists() {
            fs::remove_file(&path).expect("remove stale test file");
        }
        path
    }

    #[test]
    fn writes_reads_and_releases_fixed_slot() {
        let frame = encode_frame(FrameKind::Bytes, 0, 7, 0, 0, &[], b"hello").expect("encode");
        let mut region = FixedSlotRegion::new(2, 128).expect("region");

        region
            .write_committed(0, 7, &frame)
            .expect("write committed");
        assert_eq!(region.state(0).expect("state"), SlotState::Committed);

        let snapshot = region.read_committed(0).expect("read committed");
        assert_eq!(snapshot.state, SlotState::Pinned);
        assert_eq!(snapshot.seq, 7);
        assert_eq!(
            decode_frame(&snapshot.data).expect("decode").payload,
            b"hello"
        );
        assert_eq!(region.state(0).expect("state"), SlotState::Pinned);

        region.release_pinned(0).expect("release");
        assert_eq!(region.state(0).expect("state"), SlotState::Free);
    }

    #[test]
    fn rejects_write_to_non_free_slot() {
        let frame = encode_frame(FrameKind::Bytes, 0, 1, 0, 0, &[], b"a").expect("encode");
        let mut region = FixedSlotRegion::new(1, 128).expect("region");
        region.write_committed(0, 1, &frame).expect("write");

        assert_eq!(
            region.write_committed(0, 2, &frame).expect_err("full"),
            DslineError::Channel(ChannelError::BufferFull)
        );
    }

    #[test]
    fn rejects_oversized_frame() {
        let mut region = FixedSlotRegion::new(1, 4).expect("region");

        assert_eq!(
            region
                .write_committed(0, 1, b"abcde")
                .expect_err("too large"),
            DslineError::Channel(ChannelError::MessageTooLarge {
                len: 5,
                slot_size: 4
            })
        );
    }

    #[test]
    fn marks_corrupted_slot() {
        let mut region = FixedSlotRegion::new(1, 64).expect("region");

        region.mark_corrupted(0).expect("mark");

        assert_eq!(region.state(0).expect("state"), SlotState::Corrupted);
    }

    fn channel(capacity: usize, slot_size: usize) -> ShmSpscChannel {
        ShmSpscChannel::new(SpscConfig {
            capacity,
            slot_size,
            backpressure: Backpressure::Raise,
            timeout: None,
        })
        .expect("channel")
    }

    #[test]
    fn channel_sends_and_receives_bytes_in_order() {
        let mut channel = channel(2, 16);

        channel.send(b"one").expect("send one");
        channel.send(b"two").expect("send two");

        assert_eq!(channel.recv().expect("recv one"), b"one");
        assert_eq!(channel.recv().expect("recv two"), b"two");
        assert!(channel.is_empty());
    }

    #[test]
    fn channel_sends_and_receives_chunked_message() {
        let mut channel = channel(4, 64);
        let payload = vec![b'x'; 80];

        channel.send(&payload).expect("send chunked");

        assert_eq!(channel.len(), 4);
        let message = channel.recv_with_seq().expect("recv chunked");
        assert_eq!(message.seq, 0);
        assert_eq!(message.payload, payload);
        assert!(channel.is_empty());
        assert_eq!(channel.expected_recv_sequence(), 1);
    }

    #[test]
    fn channel_rejects_chunked_message_larger_than_capacity() {
        let mut channel = channel(2, 64);
        let payload = vec![b'x'; 100];

        assert_eq!(
            channel.send(&payload).expect_err("too large"),
            DslineError::Channel(ChannelError::MessageTooLarge {
                len: 100,
                slot_size: 64
            })
        );
    }

    #[test]
    fn channel_returns_sequence_with_message() {
        let mut channel = channel(2, 16);

        channel.send(b"one").expect("send one");
        channel.send(b"two").expect("send two");

        let first = channel.recv_with_seq().expect("recv one");
        let second = channel.recv_with_seq().expect("recv two");

        assert_eq!(first.seq, 0);
        assert_eq!(first.payload, b"one");
        assert_eq!(second.seq, 1);
        assert_eq!(second.payload, b"two");
        assert_eq!(channel.next_sequence(), 2);
        assert_eq!(channel.expected_recv_sequence(), 2);
        assert_eq!(channel.last_received_sequence(), Some(1));
    }

    #[test]
    fn channel_sequence_wraps() {
        let mut channel = channel(2, 16);
        channel.set_next_sequence_for_test(u64::MAX);
        channel.set_expected_recv_sequence_for_test(u64::MAX);

        channel.send(b"last").expect("send max");
        channel.send(b"zero").expect("send wrapped");

        assert_eq!(channel.next_sequence(), 1);
        assert_eq!(channel.recv_with_seq().expect("recv max").seq, u64::MAX);
        assert_eq!(channel.recv_with_seq().expect("recv zero").seq, 0);
        assert_eq!(channel.expected_recv_sequence(), 1);
        assert_eq!(channel.last_received_sequence(), Some(0));
    }

    #[test]
    fn channel_wraps_head_and_tail() {
        let mut channel = channel(2, 16);

        channel.send(b"one").expect("send one");
        assert_eq!(channel.recv().expect("recv one"), b"one");
        channel.send(b"two").expect("send two");
        channel.send(b"three").expect("send three");

        assert_eq!(channel.recv().expect("recv two"), b"two");
        assert_eq!(channel.recv().expect("recv three"), b"three");
    }

    #[test]
    fn channel_keeps_payload_slot_size_separate_from_frame_slot_size() {
        let channel = channel(1, 16);

        assert_eq!(channel.payload_slot_size(), 16);
        assert_eq!(channel.region_slot_size(), 16 + FRAME_HEADER_LEN);
    }

    #[test]
    fn channel_reports_message_size_limits() {
        let channel = channel(4, 64);
        let limits = channel.message_size_limits();

        assert_eq!(limits.slot_size, 64);
        assert_eq!(limits.chunk_metadata_size, CHUNK_METADATA_LEN);
        assert_eq!(limits.chunk_payload_size, 22);
        assert_eq!(limits.max_message_size, 88);
    }

    #[test]
    fn channel_rejects_oversized_payload_before_frame_encode() {
        let mut channel = channel(1, 4);

        assert_eq!(
            channel.send(b"abcde").expect_err("too large"),
            DslineError::Channel(ChannelError::MessageTooLarge {
                len: 5,
                slot_size: 4
            })
        );
    }

    #[test]
    fn channel_reports_full_with_raise_backpressure() {
        let mut channel = channel(1, 16);
        channel.send(b"one").expect("send one");

        assert_eq!(
            channel.send(b"two").expect_err("full"),
            DslineError::Channel(ChannelError::BufferFull)
        );
    }

    #[test]
    fn channel_block_backpressure_times_out() {
        let mut channel = ShmSpscChannel::new(SpscConfig {
            capacity: 1,
            slot_size: 16,
            backpressure: Backpressure::Block,
            timeout: Some(Duration::from_millis(1)),
        })
        .expect("channel");
        channel.send(b"one").expect("send one");

        assert_eq!(
            channel.send(b"two").expect_err("timeout"),
            DslineError::Channel(ChannelError::BufferFull)
        );
    }

    #[test]
    fn channel_drop_newest_backpressure_discards_incoming_message() {
        let mut channel = ShmSpscChannel::new(SpscConfig {
            capacity: 1,
            slot_size: 16,
            backpressure: Backpressure::DropNewest,
            timeout: None,
        })
        .expect("channel");
        channel.send(b"one").expect("send one");
        channel.send(b"two").expect("drop two");

        assert_eq!(channel.len(), 1);
        assert_eq!(channel.next_sequence(), 1);
        let message = channel.recv_with_seq().expect("recv one");
        assert_eq!(message.seq, 0);
        assert_eq!(message.payload, b"one");
        assert_eq!(
            channel.recv().expect_err("empty"),
            DslineError::Channel(ChannelError::BufferEmpty)
        );
    }

    #[test]
    fn channel_drop_oldest_backpressure_discards_oldest_message() {
        let mut channel = ShmSpscChannel::new(SpscConfig {
            capacity: 2,
            slot_size: 16,
            backpressure: Backpressure::DropOldest,
            timeout: None,
        })
        .expect("channel");
        channel.send(b"one").expect("send one");
        channel.send(b"two").expect("send two");
        channel.send(b"three").expect("send three");

        assert_eq!(channel.len(), 2);
        assert_eq!(channel.next_sequence(), 3);
        assert_eq!(channel.expected_recv_sequence(), 1);
        let first = channel.recv_with_seq().expect("recv two");
        let second = channel.recv_with_seq().expect("recv three");
        assert_eq!(first.seq, 1);
        assert_eq!(first.payload, b"two");
        assert_eq!(second.seq, 2);
        assert_eq!(second.payload, b"three");
    }

    #[test]
    fn channel_drop_oldest_discards_whole_chunked_message() {
        let mut channel = ShmSpscChannel::new(SpscConfig {
            capacity: 4,
            slot_size: 64,
            backpressure: Backpressure::DropOldest,
            timeout: None,
        })
        .expect("channel");
        let large = vec![b'a'; 80];
        channel.send(&large).expect("send large");
        channel.send(b"small").expect("send small and drop large");

        assert_eq!(channel.len(), 1);
        assert_eq!(channel.expected_recv_sequence(), 1);
        let message = channel.recv_with_seq().expect("recv small");
        assert_eq!(message.seq, 1);
        assert_eq!(message.payload, b"small");
    }

    #[test]
    fn channel_close_prevents_send_and_drains_existing_messages() {
        let mut channel = channel(2, 16);
        channel.send(b"one").expect("send one");
        channel.close();

        assert_eq!(
            channel.send(b"two").expect_err("closed"),
            DslineError::Channel(ChannelError::Closed)
        );
        assert_eq!(channel.recv().expect("drain"), b"one");
        assert_eq!(
            channel.recv().expect_err("closed empty"),
            DslineError::Channel(ChannelError::Closed)
        );
    }

    #[test]
    fn channel_releases_slot_after_decode_error() {
        let mut channel = channel(1, 16);
        channel.send(b"one").expect("send one");
        channel.corrupt_tail_checksum_for_test();

        assert_eq!(
            channel.recv().expect_err("checksum mismatch"),
            DslineError::Channel(ChannelError::CorruptedMessage)
        );
        assert!(channel.is_empty());
        channel.send(b"two").expect("slot reusable");
        assert_eq!(channel.recv().expect("recv two"), b"two");
    }

    #[test]
    fn channel_rejects_frame_sequence_mismatch() {
        let mut channel = channel(1, 16);
        channel.send(b"one").expect("send one");
        channel.corrupt_tail_frame_seq_for_test(7);

        assert_eq!(
            channel.recv().expect_err("sequence mismatch"),
            DslineError::Channel(ChannelError::SequenceMismatch {
                expected: 0,
                actual: 7
            })
        );
        assert!(channel.is_empty());
    }

    #[test]
    fn channel_accepts_injected_storage_backend() {
        let config = SpscConfig {
            capacity: 2,
            slot_size: 16,
            backpressure: Backpressure::Raise,
            timeout: None,
        };
        let storage = MemorySlotStorage::new(2, 16 + FRAME_HEADER_LEN).expect("storage");
        let mut channel = ShmSpscChannel::with_storage(config, storage).expect("channel");

        channel.send(b"one").expect("send");

        assert_eq!(channel.recv().expect("recv"), b"one");
    }

    #[test]
    fn channel_rejects_storage_capacity_mismatch() {
        let config = SpscConfig {
            capacity: 2,
            slot_size: 16,
            backpressure: Backpressure::Raise,
            timeout: None,
        };
        let storage = MemorySlotStorage::new(1, 16 + FRAME_HEADER_LEN).expect("storage");

        assert_eq!(
            ShmSpscChannel::with_storage(config, storage).expect_err("mismatch"),
            DslineError::Channel(ChannelError::InvalidConfig("storage capacity mismatch"))
        );
    }

    #[test]
    fn channel_rejects_storage_slot_size_too_small() {
        let config = SpscConfig {
            capacity: 1,
            slot_size: 16,
            backpressure: Backpressure::Raise,
            timeout: None,
        };
        let storage = MemorySlotStorage::new(1, 16).expect("storage");

        assert_eq!(
            ShmSpscChannel::with_storage(config, storage).expect_err("too small"),
            DslineError::Channel(ChannelError::InvalidConfig(
                "storage slot_size is too small"
            ))
        );
    }

    #[test]
    fn file_storage_writes_reads_and_clears_slot() {
        let path = test_file_path("file-storage-basic");
        let mut storage = FileSlotStorage::create(&path, 2, 16, 32).expect("storage");

        storage.write_slot(1, b"hello").expect("write");
        assert_eq!(storage.read_slot(1, 5).expect("read"), b"hello");

        storage.clear_slot(1).expect("clear");
        assert_eq!(storage.read_slot(1, 5).expect("read cleared"), &[0; 5]);

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn file_storage_can_back_channel() {
        let path = test_file_path("file-storage-channel");
        let config = SpscConfig {
            capacity: 2,
            slot_size: 16,
            backpressure: Backpressure::Raise,
            timeout: None,
        };
        let storage =
            FileSlotStorage::create(&path, 2, 16 + FRAME_HEADER_LEN, 2 * (16 + FRAME_HEADER_LEN))
                .expect("storage");
        let mut channel = ShmSpscChannel::with_storage(config, storage).expect("channel");

        channel.send(b"one").expect("send one");
        channel.send(b"two").expect("send two");

        assert_eq!(channel.recv().expect("recv one"), b"one");
        assert_eq!(channel.recv().expect("recv two"), b"two");

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn file_storage_rejects_smaller_existing_file() {
        let path = test_file_path("file-storage-small");
        fs::write(&path, b"tiny").expect("write tiny file");

        assert_eq!(
            FileSlotStorage::open(&path, 2, 16, 32).expect_err("too small"),
            DslineError::Channel(ChannelError::InvalidConfig(
                "file storage is smaller than expected"
            ))
        );

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn persistent_channel_drop_oldest_backpressure_discards_oldest_message() {
        let path = test_file_path("persistent-drop-oldest");
        let mut channel = PersistentShmChannel::<MmapSlotStorage>::create(
            &path,
            SpscConfig {
                capacity: 2,
                slot_size: 16,
                backpressure: Backpressure::DropOldest,
                timeout: None,
            },
        )
        .expect("persistent channel");

        channel.send(b"one").expect("send one");
        channel.send(b"two").expect("send two");
        channel.send(b"three").expect("send three");

        assert_eq!(channel.len(), 2);
        let first = channel.recv_with_seq().expect("recv two");
        let second = channel.recv_with_seq().expect("recv three");
        assert_eq!(first.seq, 1);
        assert_eq!(first.payload, b"two");
        assert_eq!(second.seq, 2);
        assert_eq!(second.payload, b"three");

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn persistent_channel_sends_and_receives_chunked_message() {
        let path = test_file_path("persistent-chunked");
        let mut channel = PersistentShmChannel::<MmapSlotStorage>::create(
            &path,
            SpscConfig {
                capacity: 4,
                slot_size: 64,
                backpressure: Backpressure::Raise,
                timeout: None,
            },
        )
        .expect("persistent channel");
        let payload = vec![b'z'; 80];

        channel.send(&payload).expect("send chunked");

        assert_eq!(channel.len(), 4);
        let message = channel.recv_with_seq().expect("recv chunked");
        assert_eq!(message.seq, 0);
        assert_eq!(message.payload, payload);
        assert!(channel.is_empty());

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn mmap_storage_writes_reads_and_clears() {
        let path = test_file_path("mmap-storage-basic");
        let storage = MmapSlotStorage::create(&path, 2, 16, 32).expect("storage");

        storage.write_at(1, b"hello").expect("write");
        assert_eq!(storage.read_at(1, 5).expect("read"), b"hello");

        storage.clear_at(1, 16).expect("clear");
        assert_eq!(storage.read_at(1, 5).expect("read cleared"), &[0; 5]);

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn mmap_storage_two_instances_share_data() {
        let path = test_file_path("mmap-storage-shared");
        let created = MmapSlotStorage::create(&path, 2, 16, 32).expect("storage");

        created.write_at(5, b"hello").expect("write");

        // A second handle to the same file observes the first handle's writes
        // through the shared mapping — the basis of cross-process sharing.
        let opened = MmapSlotStorage::open(&path, 2, 16, 32).expect("open");
        assert_eq!(opened.read_at(5, 5).expect("read"), b"hello");

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn mmap_storage_rejects_smaller_existing_file() {
        let path = test_file_path("mmap-storage-small");
        fs::write(&path, b"tiny").expect("write tiny file");

        assert_eq!(
            MmapSlotStorage::open(&path, 2, 16, 32).expect_err("too small"),
            DslineError::Channel(ChannelError::InvalidConfig(
                "file storage is smaller than expected"
            ))
        );

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn mmap_storage_create_new_refuses_existing_file() {
        let path = test_file_path("mmap-storage-create-new");
        let created = MmapSlotStorage::create(&path, 2, 16, 32).expect("storage");
        created.write_at(0, b"keep me").expect("write");

        assert!(MmapSlotStorage::create_new(&path, 2, 16, 32).is_err());

        let opened = MmapSlotStorage::open(&path, 2, 16, 32).expect("open");
        assert_eq!(opened.read_at(0, 7).expect("read"), b"keep me");

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn persistent_channel_mmap_backend_recovers_messages() {
        let path = test_file_path("persistent-mmap-recover");
        let config = SpscConfig {
            capacity: 4,
            slot_size: 64,
            backpressure: Backpressure::Raise,
            timeout: None,
        };
        let mut created =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone()).expect("create");
        created.send(b"one").expect("send one");
        created.send(b"two").expect("send two");

        // A second handle to the same region recovers the queued messages.
        let mut opened =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config).expect("open");
        assert_eq!(opened.len(), 2);
        assert_eq!(opened.recv_with_seq().expect("recv one").payload, b"one");
        assert_eq!(opened.recv_with_seq().expect("recv two").payload, b"two");
        assert!(opened.is_empty());

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn persistent_channel_create_or_open_reuses_existing_region() {
        let path = test_file_path("persistent-mmap-create-or-open");
        let config = SpscConfig {
            capacity: 2,
            slot_size: 16,
            backpressure: Backpressure::Raise,
            timeout: None,
        };
        let mut first =
            PersistentShmChannel::<MmapSlotStorage>::create_or_open(&path, config.clone())
                .expect("first handle");
        first.send(b"hello").expect("send");

        // A second create_or_open must open the existing region, not truncate it.
        let mut second = PersistentShmChannel::<MmapSlotStorage>::create_or_open(&path, config)
            .expect("second handle");
        assert_eq!(second.recv_with_seq().expect("recv").payload, b"hello");

        fs::remove_file(path).expect("cleanup");
    }

    // ── cursor protocol tests ──

    fn persistent_config(capacity: usize, slot_size: usize) -> SpscConfig {
        SpscConfig {
            capacity,
            slot_size,
            backpressure: Backpressure::Raise,
            timeout: None,
        }
    }

    #[test]
    fn persistent_channel_two_handles_share_cursor_ring() {
        // Two handles to the same region (the in-process equivalent of two
        // processes): ring position comes from the shared cursors, so the
        // consumer handle sees exactly what the producer handle committed.
        let path = test_file_path("persistent-cursor-roundtrip");
        let config = persistent_config(8, 64);
        let mut producer =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone()).expect("create");
        let large = vec![b'z'; 80]; // 4-slot chunked message
        producer.send(b"one").expect("send one");
        producer.send(&large).expect("send chunked");
        producer.send(b"two").expect("send two");

        let mut consumer =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config).expect("open consumer");
        assert_eq!(consumer.len(), 6, "depth = committed slots via cursors");

        let first = consumer.recv_with_seq().expect("recv one");
        assert_eq!((first.seq, first.payload), (0, b"one".to_vec()));
        let second = consumer.recv_with_seq().expect("recv chunked");
        assert_eq!(second.seq, 1);
        assert_eq!(second.payload, large);
        let third = consumer.recv_with_seq().expect("recv two");
        assert_eq!((third.seq, third.payload), (2, b"two".to_vec()));
        assert!(consumer.is_empty());

        // The producer observes the released space through the shared tail
        // cursor on its next operation.
        producer.send(b"three").expect("send three");
        assert_eq!(
            consumer.recv_with_seq().expect("recv three").payload,
            b"three"
        );

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn persistent_channel_recovery_skips_crashed_writing_slot() {
        // A producer that died mid-write leaves a WRITING slot that the
        // shared head cursor never counted. Recovery must count it in the
        // depth (bias), skip it for the next write, and surface it to the
        // consumer as an empty ring instead of a hang.
        let path = test_file_path("persistent-writer-crash");
        let config = persistent_config(4, 64);
        let mut ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone()).expect("create");
        ch.send(b"one").expect("send");
        // Crash: slot 1 holds an in-flight WRITING header (seq 1); the head
        // cursor still points at slot 1, which is never committed.
        ch.region
            .write_header(1, SlotState::Writing, 1, 10)
            .expect("zombie header");

        let mut recovered =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config).expect("open after crash");
        // committed "one" (cursor-counted) + the zombie (bias).
        assert_eq!(recovered.len(), 2);
        assert_eq!(
            recovered.next_sequence(),
            2,
            "continue after the zombie seq"
        );

        // The write position steps over the zombie (slot 2, not slot 1).
        recovered.send(b"two").expect("send past zombie");
        assert_eq!(recovered.region.load_head().expect("head"), 2);

        assert_eq!(recovered.recv_with_seq().expect("recv one").payload, b"one");
        // The zombie is surfaced as an empty ring, not an infinite poll.
        assert!(matches!(
            recovered.recv(),
            Err(DslineError::Channel(ChannelError::BufferEmpty))
        ));

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn persistent_channel_send_guard_counts_crash_leftovers() {
        // With the zombie occupying physical capacity, the send guard must
        // not allow an over-commit even though the cursors under-count.
        let path = test_file_path("persistent-guard-bias");
        let config = persistent_config(2, 64);
        let ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone()).expect("create");
        let frame = encode_frame(FrameKind::Bytes, 0, 0, 0, 0, &[], b"one").expect("encode");
        ch.region
            .write_committed(0, 0, &frame)
            .expect("commit slot 0");
        ch.region
            .write_header(1, SlotState::Writing, 1, 10)
            .expect("zombie header");

        let mut recovered =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config).expect("open after crash");
        // Only slot 2... capacity 2: the ring is full (committed + zombie).
        assert_eq!(recovered.len(), 2);
        assert!(matches!(
            recovered.send(b"full?"),
            Err(DslineError::Channel(ChannelError::BufferFull))
        ));

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn persistent_channel_writing_only_ring_reads_as_empty() {
        // A region whose only occupied slot is WRITING (producer mid-write
        // of the very first message) must behave as an empty ring.
        let path = test_file_path("persistent-writing-only");
        let config = persistent_config(4, 64);
        let ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone()).expect("create");
        ch.region
            .write_header(0, SlotState::Writing, 0, 10)
            .expect("in-flight header");

        let mut other = PersistentShmChannel::<MmapSlotStorage>::open(&path, config).expect("open");
        assert!(matches!(
            other.recv(),
            Err(DslineError::Channel(ChannelError::BufferEmpty))
        ));

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn persistent_channel_rejoined_handles_continue_after_drain() {
        // After both sides drain the ring and reopen, the shared cursors
        // keep the ring position; new handles continue from there with the
        // sequences restarting (the empty-open convention).
        let path = test_file_path("persistent-rejoin");
        let config = persistent_config(4, 16);
        {
            let mut producer =
                PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone())
                    .expect("create");
            producer.send(b"a").expect("send a");
            producer.send(b"b").expect("send b");
            let mut consumer = PersistentShmChannel::<MmapSlotStorage>::open(&path, config.clone())
                .expect("open consumer");
            assert_eq!(consumer.recv().expect("recv a"), b"a");
            assert_eq!(consumer.recv().expect("recv b"), b"b");
            assert!(consumer.is_empty());
        }

        let mut p = PersistentShmChannel::<MmapSlotStorage>::open(&path, config.clone())
            .expect("reopen producer");
        let mut c =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config).expect("reopen consumer");
        p.send(b"x").expect("send x");
        let message = c.recv_with_seq().expect("recv x");
        assert_eq!((message.seq, message.payload), (0, b"x".to_vec()));

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn shm_region_path_is_stable_for_the_same_name() {
        let first = shm_region_path("demo").expect("path one");
        let second = shm_region_path("demo").expect("path two");
        let other = shm_region_path("other").expect("path other");

        assert_eq!(first, second);
        assert_ne!(first, other);

        let parent = first.parent().expect("parent dir");
        assert!(parent.ends_with("shm"), "runtime dir: {parent:?}");
    }
}
