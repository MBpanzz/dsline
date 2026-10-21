//! Shared-memory backend.
//!
//! The 0.0.1 target is a Linux-first fixed-slot SPSC bytes backend. The public
//! API is intentionally conservative until ADR-001 is accepted.

use dsline_core::{
    checksum::checksum32, decode_frame, decode_frame_header, encode_frame, encode_frame_header,
    Backpressure, ChannelError, DslineError, Frame, FrameKind, Metadata, Result, SpscConfig,
    CHUNK_METADATA_LEN, FRAME_FLAG_CHUNKED, FRAME_FLAG_CHUNK_END, FRAME_FLAG_CHUNK_START,
    FRAME_HEADER_LEN, TLV_CHUNK_INDEX, TLV_CHUNK_MESSAGE_LEN, TLV_CHUNK_TOTAL,
};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{fence, AtomicU64, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use memmap2::MmapMut;

pub mod process;

pub use process::{current_process_identity, process_liveness, ProcessIdentity, ProcessLiveness};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SlotState {
    Free = 0,
    Writing = 1,
    Committed = 2,
    Pinned = 3,
    Corrupted = 4,
}

impl SlotState {
    fn from_byte(raw: u8) -> SlotState {
        match raw {
            0 => SlotState::Free,
            1 => SlotState::Writing,
            2 => SlotState::Committed,
            3 => SlotState::Pinned,
            _ => SlotState::Corrupted,
        }
    }
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

    /// Open the region if it exists, otherwise create it.
    ///
    /// Returns `(storage, created)`. Named backends use an atomic
    /// create-or-open (the OS guarantees exactly one creator); the default
    /// implementation is open-then-create, which can truncate a store that
    /// another process is racing with and is only suitable for reference
    /// backends.
    fn create_or_open(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<(Self, bool)> {
        match Self::open(path.as_ref(), capacity, slot_size, total_len) {
            Ok(storage) => Ok((storage, false)),
            Err(_) => Ok((Self::create(path, capacity, slot_size, total_len)?, true)),
        }
    }

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

    /// Compare-and-exchange an 8-byte word at `offset`.
    ///
    /// Returns `(current, true)` when the word equaled `expected` and was
    /// replaced with `new`, or `(current, false)` otherwise (with the word
    /// left unchanged). Used by the MPSC reservation protocol, where several
    /// producers must atomically claim disjoint ranges of the head cursor.
    ///
    /// Default implementation: a non-atomic check-then-write with SeqCst
    /// fences. Only safe for the single-process reference backends (the
    /// file-based store) — the shared-memory backends override it with a
    /// hardware CAS.
    fn compare_exchange_u64(&self, offset: usize, expected: u64, new: u64) -> Result<(u64, bool)> {
        let current = self.load_u64(offset)?;
        if current == expected {
            self.store_u64(offset, new)?;
            Ok((new, true))
        } else {
            Ok((current, false))
        }
    }

    /// Run `f` over `len` bytes of the backing store starting at `offset`,
    /// without copying them out first where the backend allows it.
    ///
    /// This is the zero-copy read window: `publish` computes the payload
    /// checksum through it, so a published buffer's bytes are read for
    /// verification in place instead of being duplicated into a temporary.
    /// The borrow lives only for the call — backends that must lock their
    /// mapping hold the lock for exactly the duration of `f`.
    ///
    /// Default: copies through `read_at` (file backends, where the "mapping"
    /// is a syscall read anyway). The mmap and section backends override
    /// this with a true in-place slice.
    fn with_slice<R>(&self, offset: usize, len: usize, f: impl FnOnce(&[u8]) -> R) -> Result<R> {
        let data = self.read_at(offset, len)?;
        Ok(f(&data))
    }
}

/// Length of an opened file storage, tolerating a concurrent creator that is
/// still resizing it.
///
/// On the file backends `create_or_open` arbitrates the single creator with
/// `create_new`, which publishes the path *before* the creator's `set_len`
/// runs — a peer that opens in that window sees a short (often zero-length)
/// file. Wait it out with the same bounded spin the region-level
/// `wait_initialized` uses instead of failing the open; a genuinely
/// mismatched configuration (a channel opened with a larger capacity than it
/// was created with) still reports the short length after the spin.
fn wait_for_full_size(file: &File, total_len: usize) -> Result<u64> {
    let stat = || -> Result<u64> {
        Ok(file
            .metadata()
            .map_err(|_| ChannelError::StorageIo("stat file storage"))?
            .len())
    };
    let mut actual_len = stat()?;
    for _ in 0..2048 {
        if actual_len >= total_len as u64 {
            break;
        }
        thread::yield_now();
        actual_len = stat()?;
    }
    Ok(actual_len)
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
        let actual_len = wait_for_full_size(&file, total_len)?;
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
        let actual_len = wait_for_full_size(&file, total_len)?;
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

    fn create_or_open(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<(Self, bool)> {
        match Self::create_new(&path, capacity, slot_size, total_len) {
            Ok(storage) => Ok((storage, true)),
            Err(_) => Ok((Self::open(path, capacity, slot_size, total_len)?, false)),
        }
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

    fn with_slice<R>(&self, offset: usize, len: usize, f: impl FnOnce(&[u8]) -> R) -> Result<R> {
        self.validate_range(offset, len)?;
        let map = self
            .map
            .lock()
            .map_err(|_| ChannelError::StorageIo("mmap storage lock poisoned"))?;
        Ok(f(&map[offset..offset + len]))
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

    fn compare_exchange_u64(&self, offset: usize, expected: u64, new: u64) -> Result<(u64, bool)> {
        self.validate_range(offset, 8)?;
        let mut map = self
            .map
            .lock()
            .map_err(|_| ChannelError::StorageIo("mmap storage lock poisoned"))?;
        // SAFETY: as in `load_u64`; the CAS is the MPSC reservation
        // linearization point — exactly one producer wins per head value.
        let ptr = unsafe { map.as_mut_ptr().add(offset) } as *mut AtomicU64;
        let current =
            unsafe { (&*ptr).compare_exchange(expected, new, Ordering::AcqRel, Ordering::Acquire) };
        match current {
            Ok(actual) => Ok((actual, true)),
            Err(actual) => Ok((actual, false)),
        }
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

// ── Named paging-file section backend (Windows) ──

/// A slot region backed by a named paging-file section.
///
/// `CreateFileMappingW` with `INVALID_HANDLE_VALUE` backs the region with
/// the system paging file: every process that opens the same section name
/// shares the same physical pages, and the memory manager keeps the views
/// coherent **without** `FlushViewOfFile` — unlike file-backed mappings,
/// whose cross-process visibility on Windows requires a flush (measured at
/// ~45 µs per call, see `docs/benchmarks.md`). This is the same mechanism
/// `multiprocessing.shared_memory` uses, minus the file: the section lives
/// as long as any handle or view is open and is destroyed when the last one
/// closes — no backing files, no stale-region cleanup.
///
/// The section name is derived from the resolved region path's file name
/// (e.g. `Local\dsline-3dd8a08e.bin`), so named channels keep their stable
/// name → region mapping. The backing store is zero-filled at creation,
/// which is already the empty-ring state (FREE = 0); the channel still runs
/// its initialization pass and publishes the `REGION_MAGIC` last, and
/// openers wait for it (see `PersistentSlotRegion::wait_initialized`).
#[cfg(windows)]
#[derive(Debug)]
pub struct SectionShmSlotStorage {
    capacity: usize,
    slot_size: usize,
    total_len: usize,
    path: PathBuf,
    /// The section handle (`CreateFileMappingW` / `OpenFileMappingW`
    /// result), kept for the lifetime of the storage.
    section: usize,
    /// The mapped view (base address + length), guarded by the mutex.
    view: Mutex<SectionView>,
}

#[cfg(windows)]
#[derive(Debug)]
struct SectionView {
    base: usize,
}

#[cfg(windows)]
mod section_ffi {
    use std::os::raw::c_void;

    #[allow(non_camel_case_types)]
    type HANDLE = *mut c_void;

    pub const INVALID_HANDLE_VALUE: HANDLE = -1isize as HANDLE;
    pub const PAGE_READWRITE: u32 = 0x04;
    pub const FILE_MAP_ALL_ACCESS: u32 = 0x000F_001F;
    pub const ERROR_ALREADY_EXISTS: u32 = 183;

    #[link(name = "kernel32")]
    extern "system" {
        pub fn CreateFileMappingW(
            h_file: HANDLE,
            lp_attributes: *mut c_void,
            fl_protect: u32,
            dw_maximum_size_high: u32,
            dw_maximum_size_low: u32,
            lp_name: *const u16,
        ) -> HANDLE;
        pub fn OpenFileMappingW(
            dw_desired_access: u32,
            b_inherit_handle: i32,
            lp_name: *const u16,
        ) -> HANDLE;
        pub fn MapViewOfFile(
            h_file_mapping: HANDLE,
            dw_desired_access: u32,
            dw_file_offset_high: u32,
            dw_file_offset_low: u32,
            dw_number_of_bytes_to_map: usize,
        ) -> *mut c_void;
        pub fn UnmapViewOfFile(lp_base_address: *const c_void) -> i32;
        pub fn CloseHandle(h_object: HANDLE) -> i32;
        pub fn GetLastError() -> u32;
    }
}

#[cfg(windows)]
impl SectionShmSlotStorage {
    /// Section name derived from the resolved region path's file name,
    /// session-scoped (`Local\`).
    fn section_name(path: &Path) -> Result<Vec<u16>> {
        let file = path
            .file_name()
            .ok_or(ChannelError::StorageIo("region path has no file name"))?;
        let name = format!("Local\\dsline-{}", file.to_string_lossy());
        Ok(name.encode_utf16().chain(std::iter::once(0)).collect())
    }

    fn validate(total_len: usize, capacity: usize, slot_size: usize) -> Result<()> {
        if capacity == 0 {
            return Err(ChannelError::InvalidConfig("capacity must be greater than zero").into());
        }
        if slot_size == 0 {
            return Err(ChannelError::InvalidConfig("slot_size must be greater than zero").into());
        }
        let expected = capacity
            .checked_mul(slot_size)
            .and_then(|v| v.checked_add(REGION_HEADER_LEN))
            .ok_or(ChannelError::InvalidConfig("storage size overflow"))?;
        if total_len != expected {
            return Err(ChannelError::InvalidConfig("storage size mismatch").into());
        }
        Ok(())
    }

    /// Map `total_len` bytes of the section into this process.
    ///
    /// # Safety
    /// The section handle must be valid for the lifetime of the returned
    /// view; the view is unmapped in `Drop`.
    fn map_view(section: usize, total_len: usize) -> Result<SectionView> {
        let base = unsafe {
            section_ffi::MapViewOfFile(
                section as *mut std::ffi::c_void,
                section_ffi::FILE_MAP_ALL_ACCESS,
                0,
                0,
                total_len,
            )
        };
        if base.is_null() {
            return Err(ChannelError::StorageIo("map shared memory section").into());
        }
        Ok(SectionView {
            base: base as usize,
        })
    }

    fn create_or_open_impl(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<(Self, bool)> {
        Self::validate(total_len, capacity, slot_size)?;
        let path = path.as_ref().to_path_buf();
        let name = Self::section_name(&path)?;
        let section = unsafe {
            let hi = (total_len as u64 >> 32) as u32;
            let lo = total_len as u32;
            section_ffi::CreateFileMappingW(
                section_ffi::INVALID_HANDLE_VALUE,
                std::ptr::null_mut(),
                section_ffi::PAGE_READWRITE,
                hi,
                lo,
                name.as_ptr(),
            )
        };
        if section.is_null() {
            return Err(ChannelError::StorageIo("create shared memory section").into());
        }
        let created = unsafe { section_ffi::GetLastError() } != section_ffi::ERROR_ALREADY_EXISTS;
        let view = Self::map_view(section as usize, total_len)?;
        Ok((
            Self {
                capacity,
                slot_size,
                total_len,
                path,
                section: section as usize,
                view: Mutex::new(view),
            },
            created,
        ))
    }

    fn open_impl(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<Self> {
        Self::validate(total_len, capacity, slot_size)?;
        let path = path.as_ref().to_path_buf();
        let name = Self::section_name(&path)?;
        let section = unsafe {
            section_ffi::OpenFileMappingW(section_ffi::FILE_MAP_ALL_ACCESS, 0, name.as_ptr())
        };
        if section.is_null() {
            return Err(ChannelError::StorageIo("open shared memory section").into());
        }
        let view = Self::map_view(section as usize, total_len)?;
        Ok(Self {
            capacity,
            slot_size,
            total_len,
            path,
            section: section as usize,
            view: Mutex::new(view),
        })
    }

    fn validate_range(&self, offset: usize, len: usize) -> Result<()> {
        if offset > self.total_len || len > self.total_len - offset {
            return Err(ChannelError::InvalidConfig("section storage range out of bounds").into());
        }
        Ok(())
    }
}

#[cfg(windows)]
impl Drop for SectionShmSlotStorage {
    fn drop(&mut self) {
        let view = self
            .view
            .lock()
            .map_err(|_| ChannelError::StorageIo("section storage lock poisoned"))
            .ok();
        if let Some(view) = view {
            if view.base != 0 {
                // SAFETY: the view was produced by `MapViewOfFile` and no
                // operation can outlive the storage (the mutex guard is
                // held here, and `self` is being dropped).
                unsafe {
                    section_ffi::UnmapViewOfFile(view.base as *const std::ffi::c_void);
                }
            }
        }
        if self.section != 0 {
            // SAFETY: the section handle was produced by
            // `CreateFileMappingW` / `OpenFileMappingW` and is only closed
            // once, here.
            unsafe {
                section_ffi::CloseHandle(self.section as *mut std::ffi::c_void);
            }
        }
    }
}

#[cfg(windows)]
impl SlotRegionIo for SectionShmSlotStorage {
    fn create(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<Self> {
        // A named section cannot be truncated while other processes hold
        // it; `create` is create-or-open with the flag dropped.
        Ok(Self::create_or_open_impl(path, capacity, slot_size, total_len)?.0)
    }

    fn open(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<Self> {
        Self::open_impl(path, capacity, slot_size, total_len)
    }

    fn create_or_open(
        path: impl AsRef<Path>,
        capacity: usize,
        slot_size: usize,
        total_len: usize,
    ) -> Result<(Self, bool)> {
        Self::create_or_open_impl(path, capacity, slot_size, total_len)
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
        let view = self
            .view
            .lock()
            .map_err(|_| ChannelError::StorageIo("section storage lock poisoned"))?;
        // SAFETY: the view outlives the guard; `offset + len` is within the
        // mapped range (validated above).
        let src = unsafe { std::slice::from_raw_parts((view.base + offset) as *const u8, len) };
        Ok(src.to_vec())
    }

    fn with_slice<R>(&self, offset: usize, len: usize, f: impl FnOnce(&[u8]) -> R) -> Result<R> {
        self.validate_range(offset, len)?;
        let view = self
            .view
            .lock()
            .map_err(|_| ChannelError::StorageIo("section storage lock poisoned"))?;
        // SAFETY: as in `read_at`; the slice borrow ends before the guard
        // is released, so no reference outlives the mapping view.
        let src = unsafe { std::slice::from_raw_parts((view.base + offset) as *const u8, len) };
        Ok(f(src))
    }

    fn write_at(&self, offset: usize, bytes: &[u8]) -> Result<()> {
        self.validate_range(offset, bytes.len())?;
        let view = self
            .view
            .lock()
            .map_err(|_| ChannelError::StorageIo("section storage lock poisoned"))?;
        // SAFETY: as in `read_at`.
        let dst =
            unsafe { std::slice::from_raw_parts_mut((view.base + offset) as *mut u8, bytes.len()) };
        dst.copy_from_slice(bytes);
        Ok(())
    }

    fn clear_at(&self, offset: usize, len: usize) -> Result<()> {
        self.validate_range(offset, len)?;
        let view = self
            .view
            .lock()
            .map_err(|_| ChannelError::StorageIo("section storage lock poisoned"))?;
        // SAFETY: as in `read_at`.
        let dst = unsafe { std::slice::from_raw_parts_mut((view.base + offset) as *mut u8, len) };
        dst.fill(0);
        Ok(())
    }

    // `flush_range` intentionally falls back to the no-op default: the
    // paging-file section is coherent across views without flushing.

    fn load_u64(&self, offset: usize) -> Result<u64> {
        self.validate_range(offset, 8)?;
        let view = self
            .view
            .lock()
            .map_err(|_| ChannelError::StorageIo("section storage lock poisoned"))?;
        // SAFETY: `offset` is 8-aligned (region header cursor constants) and
        // within the mapping, which outlives the lock guard; the underlying
        // memory is shared with other processes, where the hardware provides
        // the atomicity of aligned 8-byte accesses.
        let ptr = (view.base + offset) as *const AtomicU64;
        Ok(unsafe { (&*ptr).load(Ordering::Acquire) })
    }

    fn store_u64(&self, offset: usize, value: u64) -> Result<()> {
        self.validate_range(offset, 8)?;
        let view = self
            .view
            .lock()
            .map_err(|_| ChannelError::StorageIo("section storage lock poisoned"))?;
        // SAFETY: as in `load_u64`; a release store publishes the slot
        // writes that precede it.
        let ptr = (view.base + offset) as *mut AtomicU64;
        unsafe { (&*ptr).store(value, Ordering::Release) };
        Ok(())
    }

    fn fetch_max_u64(&self, offset: usize, value: u64) -> Result<u64> {
        self.validate_range(offset, 8)?;
        let view = self
            .view
            .lock()
            .map_err(|_| ChannelError::StorageIo("section storage lock poisoned"))?;
        // SAFETY: as in `load_u64`; the RMW advances the shared cursor
        // monotonically even when two roles advance it concurrently.
        let ptr = (view.base + offset) as *mut AtomicU64;
        Ok(unsafe { (&*ptr).fetch_max(value, Ordering::Release) })
    }

    fn compare_exchange_u64(&self, offset: usize, expected: u64, new: u64) -> Result<(u64, bool)> {
        self.validate_range(offset, 8)?;
        let view = self
            .view
            .lock()
            .map_err(|_| ChannelError::StorageIo("section storage lock poisoned"))?;
        // SAFETY: as in `load_u64`; the CAS is the MPSC reservation
        // linearization point (see `MmapSlotStorage::compare_exchange_u64`).
        let ptr = (view.base + offset) as *mut AtomicU64;
        let current =
            unsafe { (&*ptr).compare_exchange(expected, new, Ordering::AcqRel, Ordering::Acquire) };
        match current {
            Ok(actual) => Ok((actual, true)),
            Err(actual) => Ok((actual, false)),
        }
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

/// Size of the per-slot header stored in persistent storage:
///
/// ```text
/// [0]       state: u8
/// [1..9]    seq: u64 LE
/// [9..13]   payload_len: u32 LE
/// [13..17]  writer_pid: u32 LE
/// [17..25]  writer_start_time: u64 LE   (platform-local start timestamp)
/// [25..29]  writer_epoch: u32 LE        (per-handle diagnostic id)
/// [29..37]  writer_timestamp_ns: u64 LE (unix ns when WRITING was marked)
/// [37..41]  reserved_slots: u32 LE      (slots claimed by this message)
/// [41..45]  identity_magic: u32 LE      (published last; see below)
/// [45..48]  reserved (zero)
/// ```
///
/// The writer-identity fields are present exactly when `identity_magic`
/// equals [`SLOT_IDENTITY_MAGIC`]. The writer stores the identity fields
/// first, issues a Release fence, and publishes the magic last, so a reader
/// that observes the magic (after an Acquire fence) never sees a torn or
/// partially written identity — the property the reclaim decision relies on
/// (verify Z4 checks).
pub const PERSISTENT_SLOT_HEADER_LEN: usize = 48;

/// Identity magic published last in a WRITING slot header (bytes `WIV1`
/// little-endian).
pub const SLOT_IDENTITY_MAGIC: u32 = 0x3156_4957;

const ID_STATE_OFFSET: usize = 0;
const ID_SEQ_OFFSET: usize = 1;
const ID_PAYLOAD_LEN_OFFSET: usize = 9;
const ID_PID_OFFSET: usize = 13;
const ID_START_TIME_OFFSET: usize = 17;
const ID_EPOCH_OFFSET: usize = 25;
const ID_TIMESTAMP_OFFSET: usize = 29;
const ID_RESERVED_SLOTS_OFFSET: usize = 37;
const ID_MAGIC_OFFSET: usize = 41;

/// Size of the region header at the start of the backing store, holding the
/// SPSC cursors, the zero-copy pin floor, the initialization magic, and the
/// region format version:
///
/// ```text
/// [head: u64 @ 0] [tail: u64 @ 8] [magic: u32 @ 16] [format: u32 @ 20]
/// [pin_floor: u64 @ 24]
/// ```
///
/// `head` is producer-owned (monotonic slot count), `tail` is consumer-owned.
/// The magic is written last by the initializer (release store) and polled
/// by the open path (acquire load), so a peer can never observe a
/// half-initialized region: the caller that creates the store first fills
/// every slot header and only then publishes the magic; everyone else waits
/// for it before recovering the ring. The format word distinguishes the
/// current on-region layout (48-byte slot headers with writer identity, the
/// zero-copy pin floor) from older ones; a region written by an older build
/// is rejected at open instead of being misinterpreted.
pub const REGION_HEADER_LEN: usize = 32;

/// On-region layout version (`REGION_FORMAT` word).
///
/// 3 = 48-byte slot headers with writer identity + the `pin_floor` word
/// (zero-copy views, roadmap §4.2). Version 2 regions (no pin floor) are
/// rejected at open.
///
/// 4 = frames carry a CRC-32 (IEEE) checksum instead of the byte-at-a-time
/// FNV-1a one (see `dsline_core::checksum`). The stored checksums differ, so
/// version-3 regions are rejected at open instead of being mis-read as
/// corrupted.
pub const REGION_FORMAT: u32 = 4;

/// Magic value published once initialization completes
/// (bytes `DSL1` little-endian) — single-producer (SPSC) mode.
pub const REGION_MAGIC: u32 = 0x3153_4c44;

/// Magic for multi-producer (MPSC) mode (`DSL2` little-endian).
///
/// The mode is fixed at creation: every opener must agree on it, because
/// the send protocol differs (single producer publishes head after commit;
/// multi-producer reserves head with a CAS before writing). An opener whose
/// `multi_producer` flag does not match the creator's fails at open.
pub const REGION_MAGIC_MPSC: u32 = 0x3253_4c44;

pub fn region_magic(multi_producer: bool) -> u32 {
    if multi_producer {
        REGION_MAGIC_MPSC
    } else {
        REGION_MAGIC
    }
}

const CURSOR_HEAD_OFFSET: usize = 0;
const CURSOR_TAIL_OFFSET: usize = 8;
const MAGIC_OFFSET: usize = 16;
const FORMAT_OFFSET: usize = 20;

/// Region-header offset of the zero-copy **pin floor** (roadmap §4.2/§4.4).
///
/// The floor is the smallest cursor value whose physical slot is still
/// pinned by a live zero-copy view (`recv_view`). Slots with a cursor value
/// in `[pin_floor, tail)` are logically consumed but physically reserved:
/// a producer's send guard must never let the ring wrap onto them, or a
/// live view's bytes would be overwritten mid-read. The sentinel
/// [`PIN_FLOOR_NONE`] means "no views are pinned". The floor is
/// consumer-owned (only the consumer pins and releases views) and only
/// read by producers; every writer goes through a CAS loop
/// (`floor_toward`), so an opener's recovery recompute can never clobber a
/// concurrent pin.
pub const PIN_FLOOR_OFFSET: usize = 24;

/// `pin_floor` sentinel: no zero-copy views are pinned, so the guard's
/// effective floor is the tail cursor itself.
pub const PIN_FLOOR_NONE: u64 = u64::MAX;

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

/// A slot header snapshot including the writer identity fields. The
/// identity is meaningful only when `state` is `Writing` or `Pinned` and
/// `identity_magic == SLOT_IDENTITY_MAGIC` (writers stamp WRITING headers
/// for crash recovery, consumers stamp PINNED headers when handing out a
/// zero-copy view); every other state (or a header written by a build
/// without identity support) reports `identity: None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotHeaderFull {
    pub state: SlotState,
    pub seq: u64,
    pub payload_len: usize,
    /// Writer/consumer identity and reservation extent (`Writing`/`Pinned`
    /// slots with a published identity only).
    pub identity: Option<WriterIdentity>,
}

/// The writer identity recorded in a WRITING slot header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterIdentity {
    pub pid: u32,
    pub start_time: u64,
    /// Per-handle diagnostic id (not part of the liveness decision).
    pub epoch: u32,
    /// Unix nanoseconds when the WRITING header was marked.
    pub timestamp_ns: u64,
    /// Slots claimed by this message's reservation (the reclaim extent in
    /// multi-producer mode).
    pub reserved_slots: u32,
}

/// A slot region that stores metadata (state, seq, length) in the storage
/// layer alongside frame data, enabling cross-process access.
///
/// Slot layout in storage:
/// ```text
/// [state: u8] [seq: u64 LE] [payload_len: u32 LE] [writer identity…]
/// [frame bytes…]
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
/// [state: u8] [seq: u64 LE] [payload_len: u32 LE] [writer identity…]
/// [frame bytes…]
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
        let state = SlotState::from_byte(raw[ID_STATE_OFFSET]);
        let seq = u64::from_le_bytes(
            raw[ID_SEQ_OFFSET..ID_SEQ_OFFSET + 8]
                .try_into()
                .expect("seq slice is 8 bytes"),
        );
        let payload_len = u32::from_le_bytes(
            raw[ID_PAYLOAD_LEN_OFFSET..ID_PAYLOAD_LEN_OFFSET + 4]
                .try_into()
                .expect("payload_len slice is 4 bytes"),
        ) as usize;
        Ok((state, seq, payload_len))
    }

    /// Read the full header including the writer identity. The identity is
    /// reported only for a WRITING slot whose identity magic is published
    /// (see [`SLOT_IDENTITY_MAGIC`]); anything else reports `None`.
    pub fn read_header_full(&self, index: usize) -> Result<SlotHeaderFull> {
        let raw = self
            .storage
            .read_at(self.slot_offset(index), PERSISTENT_SLOT_HEADER_LEN)?;
        Ok(Self::parse_header_full(&raw))
    }

    /// Parse a full header from raw bytes. Visible for tests.
    fn parse_header_full(raw: &[u8]) -> SlotHeaderFull {
        let state = SlotState::from_byte(raw[ID_STATE_OFFSET]);
        let seq = u64::from_le_bytes(
            raw[ID_SEQ_OFFSET..ID_SEQ_OFFSET + 8]
                .try_into()
                .expect("seq slice is 8 bytes"),
        );
        let payload_len = u32::from_le_bytes(
            raw[ID_PAYLOAD_LEN_OFFSET..ID_PAYLOAD_LEN_OFFSET + 4]
                .try_into()
                .expect("payload_len slice is 4 bytes"),
        ) as usize;
        let magic = u32::from_le_bytes(
            raw[ID_MAGIC_OFFSET..ID_MAGIC_OFFSET + 4]
                .try_into()
                .expect("magic slice is 4 bytes"),
        );
        let identity = if magic == SLOT_IDENTITY_MAGIC {
            Some(WriterIdentity {
                pid: u32::from_le_bytes(
                    raw[ID_PID_OFFSET..ID_PID_OFFSET + 4]
                        .try_into()
                        .expect("pid slice is 4 bytes"),
                ),
                start_time: u64::from_le_bytes(
                    raw[ID_START_TIME_OFFSET..ID_START_TIME_OFFSET + 8]
                        .try_into()
                        .expect("start_time slice is 8 bytes"),
                ),
                epoch: u32::from_le_bytes(
                    raw[ID_EPOCH_OFFSET..ID_EPOCH_OFFSET + 4]
                        .try_into()
                        .expect("epoch slice is 4 bytes"),
                ),
                timestamp_ns: u64::from_le_bytes(
                    raw[ID_TIMESTAMP_OFFSET..ID_TIMESTAMP_OFFSET + 8]
                        .try_into()
                        .expect("timestamp slice is 8 bytes"),
                ),
                reserved_slots: u32::from_le_bytes(
                    raw[ID_RESERVED_SLOTS_OFFSET..ID_RESERVED_SLOTS_OFFSET + 4]
                        .try_into()
                        .expect("reserved_slots slice is 4 bytes"),
                ),
            })
        } else {
            None
        };
        SlotHeaderFull {
            state,
            seq,
            payload_len,
            identity,
        }
    }

    /// Encode a 48-byte header image.
    fn encode_header(
        state: SlotState,
        seq: u64,
        payload_len: usize,
        identity: Option<WriterIdentity>,
        publish_magic: bool,
    ) -> Vec<u8> {
        let mut header = vec![0u8; PERSISTENT_SLOT_HEADER_LEN];
        header[ID_STATE_OFFSET] = state as u8;
        header[ID_SEQ_OFFSET..ID_SEQ_OFFSET + 8].copy_from_slice(&seq.to_le_bytes());
        header[ID_PAYLOAD_LEN_OFFSET..ID_PAYLOAD_LEN_OFFSET + 4]
            .copy_from_slice(&(payload_len as u32).to_le_bytes());
        if let Some(id) = identity {
            header[ID_PID_OFFSET..ID_PID_OFFSET + 4].copy_from_slice(&id.pid.to_le_bytes());
            header[ID_START_TIME_OFFSET..ID_START_TIME_OFFSET + 8]
                .copy_from_slice(&id.start_time.to_le_bytes());
            header[ID_EPOCH_OFFSET..ID_EPOCH_OFFSET + 4].copy_from_slice(&id.epoch.to_le_bytes());
            header[ID_TIMESTAMP_OFFSET..ID_TIMESTAMP_OFFSET + 8]
                .copy_from_slice(&id.timestamp_ns.to_le_bytes());
            header[ID_RESERVED_SLOTS_OFFSET..ID_RESERVED_SLOTS_OFFSET + 4]
                .copy_from_slice(&id.reserved_slots.to_le_bytes());
            if publish_magic {
                header[ID_MAGIC_OFFSET..ID_MAGIC_OFFSET + 4]
                    .copy_from_slice(&SLOT_IDENTITY_MAGIC.to_le_bytes());
            }
        }
        header
    }

    fn write_header(
        &self,
        index: usize,
        state: SlotState,
        seq: u64,
        payload_len: usize,
    ) -> Result<()> {
        // No identity fields: the header image stays zero there, so the
        // identity magic is absent and readers treat the slot as having no
        // verifiable writer.
        let header = Self::encode_header(state, seq, payload_len, None, false);
        self.storage.write_at(self.slot_offset(index), &header)
    }

    /// Mark a slot WRITING with the writer's identity: the identity fields
    /// and a Release fence land first, the identity magic last — a reader
    /// that observes the magic (after an Acquire fence) sees the complete
    /// identity, never a torn one. The magic is also what makes the
    /// identity trustworthy: a header written without it (older build, or
    /// torn write) is never eligible for automatic reclaim.
    pub fn write_writing_header(
        &self,
        index: usize,
        seq: u64,
        payload_len: usize,
        identity: WriterIdentity,
    ) -> Result<()> {
        let fields =
            Self::encode_header(SlotState::Writing, seq, payload_len, Some(identity), false);
        self.storage.write_at(self.slot_offset(index), &fields)?;
        fence(Ordering::Release);
        let magic = SLOT_IDENTITY_MAGIC.to_le_bytes();
        self.storage
            .write_at(self.slot_offset(index) + ID_MAGIC_OFFSET, &magic)
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

    /// Load the zero-copy pin floor (acquire).
    pub fn load_floor(&self) -> Result<u64> {
        self.storage.load_u64(PIN_FLOOR_OFFSET)
    }

    /// Store the zero-copy pin floor (release). Initialization and recovery
    /// only; runtime updates go through `floor_toward`'s CAS loop.
    pub fn store_floor(&self, floor: u64) -> Result<()> {
        self.storage.store_u64(PIN_FLOOR_OFFSET, floor)
    }

    /// Lower the pin floor to `target` (the pin path): min semantics, so a
    /// concurrent writer's lower floor is never raised. Retry loop on lost
    /// CAS races.
    pub fn floor_min(&self, target: u64) -> Result<u64> {
        for _ in 0..32 {
            let current = self.load_floor()?;
            if current != PIN_FLOOR_NONE && current <= target {
                // Already at least as low as the new pin: the min wins.
                return Ok(current);
            }
            let (_, ok) = self
                .storage
                .compare_exchange_u64(PIN_FLOOR_OFFSET, current, target)?;
            if ok {
                return Ok(target);
            }
            // Another writer moved the floor meanwhile; re-evaluate.
        }
        // Surrender conservatively: report whatever is there now.
        self.load_floor()
    }

    /// Raise the pin floor to `target` **only if nobody touched it since**
    /// `expected` was observed — the release/recovery path, where a scan of
    /// the ring established that no live pin sits below `target` as of that
    /// scan. A concurrent pinner that lowered the floor during the scan
    /// makes the CAS fail and keeps its (better-informed, lower) value; a
    /// phantom floor (no pinned header behind it) is raised away, which is
    /// exactly the self-heal this exists for.
    pub fn floor_cas(&self, expected: u64, target: u64) -> Result<bool> {
        let (_, ok) = self
            .storage
            .compare_exchange_u64(PIN_FLOOR_OFFSET, expected, target)?;
        Ok(ok)
    }

    /// Raw compare-exchange over any 8-byte region word (tests, recovery).
    pub fn compare_exchange_word(
        &self,
        offset: usize,
        expected: u64,
        new: u64,
    ) -> Result<(u64, bool)> {
        self.storage.compare_exchange_u64(offset, expected, new)
    }

    /// Write a PINNED header with the consumer's identity, following the
    /// same publication protocol as `write_writing_header`: the identity
    /// fields and a Release fence land first, the identity magic last, so
    /// a reclaimer that observes the magic never sees a torn identity.
    /// The identity makes a crashed consumer's leftover pins reclaimable
    /// (roadmap §6 symmetric to dead writers).
    pub fn write_pinned_header(
        &self,
        index: usize,
        seq: u64,
        payload_len: usize,
        identity: WriterIdentity,
    ) -> Result<()> {
        let fields =
            Self::encode_header(SlotState::Pinned, seq, payload_len, Some(identity), false);
        self.storage.write_at(self.slot_offset(index), &fields)?;
        fence(Ordering::Release);
        let magic = SLOT_IDENTITY_MAGIC.to_le_bytes();
        self.storage
            .write_at(self.slot_offset(index) + ID_MAGIC_OFFSET, &magic)
    }

    /// MPSC reservation: CAS the head cursor from `expected` to `new`.
    /// Returns `(actual, true)` on success. The linearization point of the
    /// reservation protocol — exactly one producer's CAS wins per value, so
    /// reserved slot ranges are disjoint (verify W-checks).
    pub fn compare_exchange_head(&self, expected: u64, new: u64) -> Result<(u64, bool)> {
        self.storage
            .compare_exchange_u64(CURSOR_HEAD_OFFSET, expected, new)
    }

    /// MPSC DropOldest: CAS the tail cursor forward by the dropped message's
    /// slot count, so concurrent droppers never advance it twice.
    pub fn compare_exchange_tail(&self, expected: u64, new: u64) -> Result<(u64, bool)> {
        self.storage
            .compare_exchange_u64(CURSOR_TAIL_OFFSET, expected, new)
    }

    /// Flush the region header page (the cursor words). No-op for backends
    /// that are coherent without flushing; on Windows file-backed mappings
    /// this publishes an MPSC reservation so other producers' CAS attempts
    /// observe it before claiming their own ranges.
    pub fn flush_header(&self) -> Result<()> {
        self.storage.flush_range(0, REGION_HEADER_LEN)
    }

    /// Load the initialization magic (acquire).
    pub fn load_magic(&self) -> Result<u32> {
        let raw = self
            .storage
            .read_at(MAGIC_OFFSET, std::mem::size_of::<u32>())?;
        Ok(u32::from_le_bytes(
            raw.try_into().expect("magic is 4 bytes"),
        ))
    }

    /// Publish the initialization magic after all FREE headers (release).
    /// The magic encodes the channel mode (`REGION_MAGIC` for SPSC,
    /// `REGION_MAGIC_MPSC` for MPSC), so an opener with a mismatched mode
    /// fails instead of misinterpreting the ring. The format word is
    /// written just before the magic, so an opener that observes the magic
    /// also observes the format.
    fn publish_magic(&self, magic: u32) -> Result<()> {
        fence(Ordering::Release);
        self.storage
            .write_at(FORMAT_OFFSET, &REGION_FORMAT.to_le_bytes())?;
        self.storage.write_at(MAGIC_OFFSET, &magic.to_le_bytes())
    }

    /// Wait (bounded) for another process to finish initializing the region.
    ///
    /// The initializer publishes the magic only after every slot header is
    /// FREE, so an opener that observes the magic (acquire) is guaranteed to
    /// see the whole initialized region — never a half-written one. Returns
    /// an error if the store never becomes initialized (a creator that died
    /// mid-initialization, or an incompatible store). A nonzero magic that
    /// is not `expected` is an immediate mode mismatch, not a wait. After
    /// the magic appears, the format word is checked so a region written by
    /// a build with a different layout is rejected instead of misread.
    pub fn wait_initialized(&self, expected: u32) -> Result<()> {
        for _ in 0..2048 {
            let magic = self.load_magic()?;
            if magic == expected {
                return self.check_format();
            }
            if magic != 0 {
                return Err(ChannelError::InvalidConfig(
                    "region channel mode mismatch (all processes must agree on multi_producer)",
                )
                .into());
            }
            thread::yield_now();
        }
        Err(ChannelError::InvalidConfig("region is not initialized").into())
    }

    /// Load the on-region format word.
    pub fn load_format(&self) -> Result<u32> {
        let raw = self
            .storage
            .read_at(FORMAT_OFFSET, std::mem::size_of::<u32>())?;
        Ok(u32::from_le_bytes(
            raw.try_into().expect("format is 4 bytes"),
        ))
    }

    /// Reject regions whose layout version differs from this build's.
    fn check_format(&self) -> Result<()> {
        let format = self.load_format()?;
        if format != REGION_FORMAT {
            return Err(ChannelError::InvalidConfig(
                "unsupported region format (delete stale dsline region files)",
            )
            .into());
        }
        Ok(())
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
    ///
    /// `stamp` records the writer identity in the WRITING header (crash
    /// recovery). `None` writes a legacy-style header without an identity —
    /// such a slot is never auto-reclaimed.
    pub fn write_committed(
        &self,
        index: usize,
        seq: u64,
        frame: &[u8],
        stamp: Option<&WriterIdentity>,
    ) -> Result<()> {
        self.write_committed_no_flush(index, seq, frame, stamp)?;
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
    ///
    /// `stamp` is the writer identity recorded in the WRITING header
    /// (crash recovery): it lands in phase 1 with the identity magic
    /// published last, so a reclaimer can later verify the writer's
    /// process identity before freeing the slot.
    fn write_committed_no_flush(
        &self,
        index: usize,
        seq: u64,
        frame: &[u8],
        stamp: Option<&WriterIdentity>,
    ) -> Result<()> {
        let (state, _, _) = self.read_header(index)?;
        if state != SlotState::Free {
            return Err(ChannelError::BufferFull.into());
        }
        // Pairs with the Release fence in `release_slot`: the slot is truly
        // free (payload cleared) before we write into it.
        fence(Ordering::Acquire);

        // Phase 1: mark WRITING, publishing the writer identity when given
        // (identity fields + Release fence first, identity magic last).
        match stamp {
            Some(stamp) => self.write_writing_header(index, seq, frame.len(), *stamp)?,
            None => self.write_header(index, SlotState::Writing, seq, frame.len())?,
        };

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

    /// As `release_slot_no_flush`, but without the COMMITTED/PINNED state
    /// check. Only the MPSC DropOldest path uses this: its tail CAS has
    /// already linearized the discard (the winning dropper is the sole
    /// releaser of that slot), so the slot must be freed even when the
    /// consumer pinned it between the peek and the CAS.
    fn force_release_no_flush(&self, index: usize) -> Result<()> {
        self.clear_payload(index)?;
        fence(Ordering::Release);
        self.write_header(index, SlotState::Free, 0, 0)
    }

    /// Reclaim a WRITING slot whose writer identity is provably dead.
    ///
    /// The decision procedure (verify Z4 checks):
    ///
    /// 1. the slot must read WRITING in two consecutive full-header reads,
    ///    with identical identity fields — a torn or transitional header
    ///    aborts the reclaim;
    /// 2. the identity magic must be published — a header without it (older
    ///    build, torn write) is never auto-reclaimed;
    /// 3. the identity must classify as [`ProcessLiveness::Gone`] — an
    ///    `Alive` or `Unknown` classification aborts (the roadmap's
    ///    conservative default: never risk a live writer);
    ///
    /// and only then the slot is force-released to FREE.
    ///
    /// Safe against a concurrent producer: while the slot stays inside the
    /// cursor window `[tail, head)` no producer may claim it (the send
    /// guard), and the caller advances the cursors only after the physical
    /// release. The slot's ring position determines what the caller may do
    /// next; this primitive only ever frees the one slot it verified.
    pub fn reclaim_dead_writing(&self, index: usize) -> Result<bool> {
        let first = self.read_header_full(index)?;
        if first.state != SlotState::Writing {
            return Ok(false);
        }
        let Some(identity) = first.identity else {
            return Ok(false); // no published identity: never auto-reclaim
        };
        // Double-read agreement: the writer publishes the header exactly
        // once, so a stable pair of reads means the write has fully landed
        // (and that the slot is not being transitioned under us).
        let second = self.read_header_full(index)?;
        if second != first {
            return Ok(false);
        }
        if process_liveness(ProcessIdentity {
            pid: identity.pid,
            start_time: identity.start_time,
        }) != ProcessLiveness::Gone
        {
            return Ok(false);
        }
        self.force_release_no_flush(index)?;
        self.storage
            .flush_range(self.slot_offset(index), PERSISTENT_SLOT_HEADER_LEN)?;
        Ok(true)
    }

    /// Reclaim a PINNED slot left behind by a provably dead consumer.
    ///
    /// Zero-copy views pin their slot (`recv_view`) until released; a
    /// consumer that dies while holding views leaves PINNED slots behind
    /// the tail cursor. The pin floor then blocks the send guard from
    /// ever wrapping onto them — the ring would drain to zero capacity.
    /// This is the mirror of `reclaim_dead_writing` for the consumer side
    /// (roadmap §6): the same decision table applies, with the same
    /// conservative default:
    ///
    /// 1. the slot must read PINNED in two consecutive full-header reads;
    /// 2. the identity magic must be published (`write_pinned_header`);
    /// 3. the identity must classify as [`ProcessLiveness::Gone`] — a live
    ///    or unknown consumer's pin is never touched.
    ///
    /// The caller (open recovery, `reclaim_stale`) recomputes the pin
    /// floor afterwards; this primitive only frees the one verified slot.
    pub fn reclaim_dead_pinned(&self, index: usize) -> Result<bool> {
        let first = self.read_header_full(index)?;
        if first.state != SlotState::Pinned {
            return Ok(false);
        }
        let Some(identity) = first.identity else {
            return Ok(false); // no published identity: never auto-reclaim
        };
        let second = self.read_header_full(index)?;
        if second != first {
            return Ok(false);
        }
        if process_liveness(ProcessIdentity {
            pid: identity.pid,
            start_time: identity.start_time,
        }) != ProcessLiveness::Gone
        {
            return Ok(false);
        }
        self.force_release_no_flush(index)?;
        self.storage
            .flush_range(self.slot_offset(index), PERSISTENT_SLOT_HEADER_LEN)?;
        Ok(true)
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

// ── Zero-copy alloc/publish (roadmap §4.2) ──

/// Byte offset of the message payload inside a single-slot zero-copy frame:
/// slot header, then frame header (single-slot frames carry no TLV
/// metadata, so the offset is a region-layout constant).
pub const ZERO_COPY_PAYLOAD_OFFSET: usize = PERSISTENT_SLOT_HEADER_LEN + FRAME_HEADER_LEN;

/// Absolute byte offset of a slot's start (region header + `index` slots).
/// Used by the view/lease objects, which see the region through the
/// object-safe [`RegionAccess`] facade below and recompute the offset from
/// `RegionAccess::slot_size`.
fn region_slot_offset(index: usize, slot_size: usize) -> usize {
    REGION_HEADER_LEN + index * slot_size
}

/// Object-safe facade over a persistent slot region for the zero-copy
/// lease/view objects.
///
/// The Python bindings' view and buffer objects must survive across calls
/// while the channel object itself stays free, and must be non-generic
/// (PyO3 pyclasses cannot take type parameters). They therefore hold an
/// `Arc<dyn RegionAccess>` — a shared handle onto the same mapping the
/// channel uses — plus their slot's byte offsets, and drive the slot state
/// machine through this trait. Every method is `&self`: the region's
/// primitives are interior-mutable through the storage backends.
pub trait RegionAccess: std::fmt::Debug + Send + Sync {
    fn capacity(&self) -> usize;
    fn slot_size(&self) -> usize;
    /// Usable frame area of one slot (slot size minus the slot header).
    fn frame_capacity(&self) -> usize;
    fn read_at(&self, offset: usize, len: usize) -> Result<Vec<u8>>;
    fn write_at(&self, offset: usize, bytes: &[u8]) -> Result<()>;
    fn flush_range(&self, offset: usize, len: usize) -> Result<()>;
    fn load_u64(&self, offset: usize) -> Result<u64>;
    fn compare_exchange_u64(&self, offset: usize, expected: u64, new: u64) -> Result<(u64, bool)>;
    fn read_header_full(&self, index: usize) -> Result<SlotHeaderFull>;
    /// PINNED → FREE: clear the payload, release-fence, write the FREE
    /// header, flush the slot. Errors unless the slot is PINNED.
    fn release_pinned(&self, index: usize) -> Result<()>;
    /// Force any state to FREE with a payload clear (reclaim path).
    fn force_release(&self, index: usize) -> Result<()>;
}

impl<S: SlotRegionIo + Send + Sync> RegionAccess for PersistentSlotRegion<S> {
    fn capacity(&self) -> usize {
        PersistentSlotRegion::capacity(self)
    }
    fn slot_size(&self) -> usize {
        PersistentSlotRegion::slot_size(self)
    }
    fn frame_capacity(&self) -> usize {
        PersistentSlotRegion::frame_capacity(self)
    }
    fn read_at(&self, offset: usize, len: usize) -> Result<Vec<u8>> {
        self.storage.read_at(offset, len)
    }
    fn write_at(&self, offset: usize, bytes: &[u8]) -> Result<()> {
        self.storage.write_at(offset, bytes)
    }
    fn flush_range(&self, offset: usize, len: usize) -> Result<()> {
        self.storage.flush_range(offset, len)
    }
    fn load_u64(&self, offset: usize) -> Result<u64> {
        self.storage.load_u64(offset)
    }
    fn compare_exchange_u64(&self, offset: usize, expected: u64, new: u64) -> Result<(u64, bool)> {
        self.storage.compare_exchange_u64(offset, expected, new)
    }
    fn read_header_full(&self, index: usize) -> Result<SlotHeaderFull> {
        PersistentSlotRegion::read_header_full(self, index)
    }
    fn release_pinned(&self, index: usize) -> Result<()> {
        PersistentSlotRegion::release_pinned(self, index)
    }
    fn force_release(&self, index: usize) -> Result<()> {
        self.force_release_no_flush(index)?;
        self.storage
            .flush_range(self.slot_offset(index), PERSISTENT_SLOT_HEADER_LEN)
    }
    // (slot_offset is the region's own; the view helpers recompute theirs
    // from `region_slot_offset(index, slot_size)`.)
}

/// Producer-side lease from [`PersistentShmChannel::alloc`]: the reserved
/// slot's payload area, writable in place until [`sealed`](Self::is_sealed).
///
/// The bytes written here land directly in the shared region — there is no
/// intermediate frame buffer and no transport copy. [`publish`]
/// (PersistentShmChannel::publish) seals the lease: the frame header
/// (checksum included) is written over the payload, the slot transitions to
/// COMMITTED, and further writes are rejected. A lease that is dropped
/// without publishing leaves its slot WRITING — recoverable like any
/// crashed writer once this process exits; `abort` frees it immediately.
///
/// [`publish`]: PersistentShmChannel::publish
#[derive(Debug)]
pub struct ShmWriteLease {
    access: std::sync::Arc<dyn RegionAccess>,
    index: usize,
    seq: u64,
    /// Absolute offset of the payload area inside the region.
    payload_offset: usize,
    payload_len: usize,
    sealed: bool,
}

impl ShmWriteLease {
    /// Message sequence assigned at `alloc` time.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Writable payload size in bytes.
    pub fn size(&self) -> usize {
        self.payload_len
    }

    /// Ring slot this lease reserved (diagnostics).
    pub fn slot_index(&self) -> usize {
        self.index
    }

    /// Whether `publish` has sealed this lease.
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// Write `bytes` into the payload area at `offset`, directly into the
    /// shared region. Errors once the lease is sealed.
    pub fn write(&self, offset: usize, bytes: &[u8]) -> Result<()> {
        if self.sealed {
            return Err(ChannelError::InvalidConfig(
                "buffer is sealed: publish() already committed it",
            )
            .into());
        }
        if offset > self.payload_len || bytes.len() > self.payload_len - offset {
            return Err(ChannelError::InvalidConfig("write out of buffer bounds").into());
        }
        self.access.write_at(self.payload_offset + offset, bytes)
    }
}

/// Consumer-side zero-copy view from [`PersistentShmChannel::recv_view`].
///
/// The view pins its slot: the message is logically consumed (the tail
/// cursor advanced) but the slot is only recycled once the view is
/// released, so the payload bytes stay valid for as long as the view is
/// alive. Reads go straight to the shared region — `read` copies exactly
/// the requested byte range, nothing more; a consumer that inspects a few
/// header bytes of a large payload never pulls the whole message.
///
/// Releasing is mandatory for ring progress: while any view is pinned, the
/// send guard refuses to wrap the ring onto its slot (`pin_floor`). Drop
/// releases best-effort; `release()` surfaces storage errors and makes the
/// release idempotence explicit.
#[derive(Debug)]
pub struct ShmRecvView {
    access: std::sync::Arc<dyn RegionAccess>,
    index: usize,
    seq: u64,
    /// Absolute offset of the payload area inside the region.
    payload_offset: usize,
    payload_len: usize,
    /// The cursor value this slot had when it was consumed — the pin
    /// floor's unit of accounting.
    cursor_value: u64,
    released: bool,
}

impl ShmRecvView {
    /// Message sequence of the viewed slot.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Payload size in bytes.
    pub fn size(&self) -> usize {
        self.payload_len
    }

    /// Ring slot backing this view (diagnostics).
    pub fn slot_index(&self) -> usize {
        self.index
    }

    /// Whether the view has been released.
    pub fn is_released(&self) -> bool {
        self.released
    }

    /// Copy `len` bytes of the payload starting at `offset` out of the
    /// shared region. The copy is the caller's explicit choice; the
    /// transport itself never moves these bytes.
    pub fn read(&self, offset: usize, len: usize) -> Result<Vec<u8>> {
        self.check_usable()?;
        if offset > self.payload_len || len > self.payload_len - offset {
            return Err(ChannelError::InvalidConfig("read out of view bounds").into());
        }
        self.access.read_at(self.payload_offset + offset, len)
    }

    /// Copy the whole payload out of the shared region.
    pub fn read_all(&self) -> Result<Vec<u8>> {
        self.check_usable()?;
        self.access.read_at(self.payload_offset, self.payload_len)
    }

    /// Release the view: PINNED → FREE, then recompute the pin floor.
    /// Idempotence is rejected explicitly — a double release is a caller
    /// bug (the slot may already be reused by then).
    pub fn release(&mut self) -> Result<()> {
        self.check_usable()?;
        self.access.release_pinned(self.index)?;
        self.released = true;
        unpin_floor(&*self.access, self.cursor_value)?;
        Ok(())
    }

    fn check_usable(&self) -> Result<()> {
        if self.released {
            return Err(ChannelError::InvalidConfig("view is already released").into());
        }
        Ok(())
    }
}

impl Drop for ShmRecvView {
    fn drop(&mut self) {
        if !self.released {
            // Best-effort: a dropped view must not pin its slot forever.
            // Storage errors are swallowed (nothing can observe them here);
            // a failed physical release self-heals through the open-time
            // dead-pin reclaim (the identity records this process as the
            // pinner, and the process is going away or the storage is
            // failing anyway).
            let _ = self.access.release_pinned(self.index);
            let _ = unpin_floor(&*self.access, self.cursor_value);
            self.released = true;
        }
    }
}

/// Recompute the pin floor after the view owning `released_cursor` was
/// released. Fast path: only the floor's owner triggers a rescan (pins are
/// consumed in FIFO order, so a later view's release cannot expose a lower
/// pin); the scan walks forward from the released cursor to the tail
/// looking for the next still-pinned slot, bounded by the capacity.
fn unpin_floor(access: &dyn RegionAccess, released_cursor: u64) -> Result<()> {
    let entry_floor = access.load_u64(PIN_FLOOR_OFFSET)?;
    let owns_floor = entry_floor == released_cursor;
    if !owns_floor && entry_floor != PIN_FLOOR_NONE {
        // A lower pin still owns the floor: releasing this view cannot
        // raise it, so the accounting stays exact without a scan.
        return Ok(());
    }
    let tail = access.load_u64(CURSOR_TAIL_OFFSET)?;
    let cap = access.capacity() as u64;
    let lo = if owns_floor {
        released_cursor + 1
    } else {
        // Floor says "no pins" while a pinned view was just released — an
        // opener raised it over a live pin (the conservative CAS race).
        // Rescan the whole window.
        tail.saturating_sub(cap)
    };
    let start = lo.max(tail.saturating_sub(cap));
    let mut next = PIN_FLOOR_NONE;
    // Cursor values in [start, tail) map to physical slots (c % capacity);
    // the first PINNED slot in ascending cursor order is the new floor.
    for cursor in start..tail {
        let index = (cursor % cap) as usize;
        if access.read_header_full(index)?.state == SlotState::Pinned {
            next = cursor;
            break;
        }
    }
    // Single-shot raise from the entry snapshot: if a concurrent pinner
    // lowered the floor while this scan ran, the CAS fails and its lower
    // (better-informed) value stands. See `floor_cas`.
    if entry_floor != next {
        access.compare_exchange_u64(PIN_FLOOR_OFFSET, entry_floor, next)?;
    }
    Ok(())
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
    /// Shared with the zero-copy lease/view objects (they must outlive
    /// individual method calls on the channel), hence the `Arc`.
    region: std::sync::Arc<PersistentSlotRegion<S>>,
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
    /// Zero-copy pin floor mirrored from the shared region header
    /// ([`PIN_FLOOR_NONE`] when no views are pinned). Reloaded on every
    /// refresh; the send guard derives its occupancy from it (see
    /// [`Self::guard_len`]).
    floor_cursor: u64,
    /// Crash-leftover occupancy: slots occupied at open that the shared
    /// cursors do not count (a producer that died mid-write left WRITING
    /// slots, and the shared head never advanced past them). Kept in the
    /// depth so the send guard never over-commits a physically full ring.
    recovery_bias: usize,
    /// This handle's writer identity, recorded in every WRITING slot header
    /// it produces (crash recovery, roadmap §6).
    writer: process::ProcessIdentity,
    writer_epoch: u32,
    /// Slots this handle has reclaimed from provably dead writers.
    reclaimed_slots: u64,
    /// Set after a reclaim (or an open with nothing readable): the next
    /// recv adopts whatever sequence it finds instead of enforcing the
    /// strict +1 chain — a crash legitimately breaks sequence contiguity.
    resync_expected: bool,
    next_seq: u64,
    expected_recv_seq: u64,
    last_received_seq: Option<u64>,
    closed: bool,
}

/// Per-handle writer epoch: distinguishes handles within one process for
/// diagnostics only (liveness is decided from pid + start_time).
static WRITER_EPOCH_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_writer_epoch() -> u32 {
    // Mix the pid so epochs from different processes never collide.
    (WRITER_EPOCH_COUNTER.fetch_add(1, Ordering::Relaxed) ^ u64::from(std::process::id())) as u32
}

fn unix_timestamp_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

impl process::ProcessIdentity {
    /// Build the per-message WRITING-header stamp.
    fn stamp(&self, epoch: u32, reserved_slots: u32) -> WriterIdentity {
        WriterIdentity {
            pid: self.pid,
            start_time: self.start_time,
            epoch,
            timestamp_ns: unix_timestamp_ns(),
            reserved_slots,
        }
    }
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
        let region = std::sync::Arc::new(PersistentSlotRegion::<S>::create(
            path,
            config.capacity,
            region_slot_size,
        )?);
        Self::new_initialized(region, config)
    }

    /// Open an existing persistent channel.
    ///
    /// The caller is responsible for knowing whether it is the producer or
    /// consumer. This method waits for the region to be initialized (the
    /// creator publishes the `REGION_MAGIC` last), then scans the slot
    /// headers once to recover the sequence state (a fresh process must
    /// rebuild the producer's next sequence and the consumer's expected
    /// sequence from storage); ring position thereafter comes from the
    /// shared cursors.
    pub fn open(path: impl AsRef<Path>, config: SpscConfig) -> Result<Self> {
        config.validate()?;
        let region_slot_size = persistent_region_slot_size(&config)?;
        let region = std::sync::Arc::new(PersistentSlotRegion::<S>::open(
            path,
            config.capacity,
            region_slot_size,
        )?);
        region.wait_initialized(region_magic(config.multi_producer))?;
        Self::opened(region, config)
    }

    /// Build the channel state for an existing, initialized region.
    fn opened(region: std::sync::Arc<PersistentSlotRegion<S>>, config: SpscConfig) -> Result<Self> {
        let mut channel = Self {
            region,
            config,
            head: 0,
            tail: 0,
            len: 0,
            head_cursor: 0,
            tail_cursor: 0,
            floor_cursor: PIN_FLOOR_NONE,
            recovery_bias: 0,
            writer: process::current_process_identity(),
            writer_epoch: next_writer_epoch(),
            reclaimed_slots: 0,
            resync_expected: false,
            next_seq: 0,
            expected_recv_seq: 0,
            last_received_seq: None,
            closed: false,
        };
        channel.recover_from_storage()?;
        Ok(channel)
    }

    fn new_initialized(
        region: std::sync::Arc<PersistentSlotRegion<S>>,
        config: SpscConfig,
    ) -> Result<Self> {
        let channel = Self {
            region,
            config,
            head: 0,
            tail: 0,
            len: 0,
            head_cursor: 0,
            tail_cursor: 0,
            floor_cursor: PIN_FLOOR_NONE,
            recovery_bias: 0,
            writer: process::current_process_identity(),
            writer_epoch: next_writer_epoch(),
            reclaimed_slots: 0,
            resync_expected: false,
            next_seq: 0,
            expected_recv_seq: 0,
            last_received_seq: None,
            closed: false,
        };
        channel.initialize_region()?;
        Ok(channel)
    }

    /// Fill a fresh backing store with the empty-ring state and publish the
    /// mode magic last: a peer that observes the magic (acquire) is
    /// guaranteed to observe every slot header written before it (release),
    /// so concurrent openers can never see a half-initialized region.
    fn initialize_region(&self) -> Result<()> {
        // Initialise all slots as FREE.
        for i in 0..self.config.capacity {
            self.region.write_header(i, SlotState::Free, 0, 0)?;
        }
        // No zero-copy views are pinned on a fresh region. The floor word
        // must be the sentinel, not the zero-filled default: cursor 0 is a
        // legal pin value, and a zero floor would permanently shrink the
        // ring once the cursors pass it.
        self.region.store_floor(PIN_FLOOR_NONE)?;
        self.region
            .publish_magic(region_magic(self.config.multi_producer))
    }

    pub fn send(&mut self, bytes: &[u8]) -> Result<()> {
        if self.closed {
            return Err(ChannelError::Closed.into());
        }
        self.refresh_cursors()?;
        let plan = send_plan(&self.config, bytes.len())?;
        if self.config.multi_producer {
            return self.send_mpsc(bytes, plan);
        }
        if !self.prepare_send_slots(plan.needed_slots)? {
            return Ok(());
        }

        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        let frames = encode_message_frames(&self.config, plan, seq, bytes)?;
        let stamp = self
            .writer
            .stamp(self.writer_epoch, plan.needed_slots as u32);
        let first_index = self.head;
        for frame in &frames {
            self.region
                .write_committed_no_flush(self.head, seq, frame, Some(&stamp))?;
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

    /// MPSC send: claim a slot range with a CAS reservation, write the
    /// message into it, and publish with one flush. The reservation is the
    /// linearization point: concurrent producers' CAS attempts serialize on
    /// the head cursor and end up with disjoint ranges, and the send guard
    /// runs *on the reservation attempt*, so two producers can never
    /// over-commit the ring even when both observe free space at once.
    fn send_mpsc(&mut self, bytes: &[u8], plan: SendPlan) -> Result<()> {
        let Some(start) = self.reserve_slots(plan.needed_slots)? else {
            return Ok(()); // dropped (DropNewest)
        };
        // Windows file-backed mappings: publish the reservation before any
        // other producer's CAS can claim an overlapping range (a no-op on
        // section and Unix mappings, which are coherent without flushing).
        self.region.flush_header()?;

        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        let frames = encode_message_frames(&self.config, plan, seq, bytes)?;
        let stamp = self
            .writer
            .stamp(self.writer_epoch, plan.needed_slots as u32);
        let first_index = (start % self.config.capacity as u64) as usize;
        for (k, frame) in frames.iter().enumerate() {
            let index = ((start + k as u64) % self.config.capacity as u64) as usize;
            self.write_reserved_slot(index, seq, frame, &stamp)?;
        }
        // One flush publishes the batch and the reservation together; the
        // consumer's per-slot COMMITTED gate absorbs the in-flight window.
        self.region.publish_batch(&frames, first_index)?;
        self.head = (first_index + frames.len()) % self.config.capacity;
        self.len = (self.len + frames.len()).min(self.config.capacity);
        Ok(())
    }

    /// Write one frame into a reserved slot, waiting out a concurrent
    /// DropOldest release.
    ///
    /// The drop CAS advances the shared tail before the physical release
    /// (so concurrent droppers never release the same message twice), so
    /// a producer that reserved immediately after the CAS can observe
    /// the previous occupant still COMMITTED. The release completes in
    /// the winner's drop loop, so in practice this waits microseconds; the
    /// bound exists only for a releaser that died between its CAS and its
    /// release.
    ///
    /// The bound is wall-clock rather than a yield count: under CPU
    /// oversubscription (the test harness runs the suite in parallel) a
    /// *live* releaser can be descheduled long enough to exhaust a fixed
    /// number of yields, which fails an otherwise healthy `send` on a
    /// DropOldest channel — and the drop strategies document that `send`
    /// stays a successful call.
    fn write_reserved_slot(
        &self,
        index: usize,
        seq: u64,
        frame: &[u8],
        stamp: &WriterIdentity,
    ) -> Result<()> {
        const RELEASE_WAIT: Duration = Duration::from_secs(30);
        let deadline = Instant::now() + RELEASE_WAIT;
        loop {
            match self
                .region
                .write_committed_no_flush(index, seq, frame, Some(stamp))
            {
                Ok(()) => return Ok(()),
                Err(DslineError::Channel(ChannelError::BufferFull)) => {
                    if Instant::now() >= deadline {
                        return Err(ChannelError::BufferFull.into());
                    }
                    thread::yield_now();
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Claim `needed_slots` ring slots for this producer.
    ///
    /// Returns `Some(start)` (the head slot count before the reservation)
    /// on success, or `None` when the message was dropped by `DropNewest`.
    /// The guard and the reservation are one CAS: the check `head - tail +
    /// needed <= capacity` runs against freshly loaded cursors and the CAS
    /// wins only if no other producer advanced head in between, so the
    /// reservation itself enforces the ring bound.
    fn reserve_slots(&mut self, needed_slots: usize) -> Result<Option<u64>> {
        if needed_slots > self.config.capacity {
            return Err(ChannelError::MessageTooLarge {
                len: needed_slots,
                slot_size: self.config.capacity,
            }
            .into());
        }
        // One deadline for the whole reservation attempt. The retry loop can
        // lose CAS races repeatedly, so a per-wait timeout would let a `Block`
        // send wait without bound: every individual wait returns Ok as soon
        // as space appears, even if another producer claims it first.
        let deadline = match self.config.backpressure {
            Backpressure::Block => self.config.timeout.map(|t| Instant::now() + t),
            _ => None,
        };
        loop {
            self.refresh_cursors()?;
            if self.config.capacity - self.guard_len() >= needed_slots {
                let head_c = self.head_cursor;
                let (actual, ok) = self
                    .region
                    .compare_exchange_head(head_c, head_c + needed_slots as u64)?;
                if ok {
                    return Ok(Some(actual));
                }
                // Another producer won the CAS first; re-evaluate the guard
                // against the new head.
                continue;
            }
            match self.config.backpressure {
                Backpressure::Raise => return Err(ChannelError::BufferFull.into()),
                Backpressure::Block => {
                    let Some(deadline) = deadline else {
                        return Err(ChannelError::BufferFull.into());
                    };
                    self.wait_for_space_until(needed_slots, deadline)?;
                    // Re-run the guard and the reservation.
                }
                Backpressure::DropNewest => return Ok(None),
                Backpressure::DropOldest => {
                    while self.config.capacity - self.guard_len() < needed_slots {
                        self.drop_oldest_message()?;
                    }
                }
            }
        }
    }

    pub fn recv(&mut self) -> Result<Vec<u8>> {
        Ok(self.recv_with_seq()?.payload)
    }

    /// Non-blocking receive: a single attempt, without the COMMITTED grace
    /// poll.
    ///
    /// `Ok(None)` means nothing is readable *right now* — the tail slot is
    /// not COMMITTED (an empty ring, or a producer mid-batch whose head
    /// cursor is not published yet). A polling consumer pays one header read
    /// per attempt instead of `recv`'s grace poll plus a Python exception, so
    /// this is the receive to spin on. Crash recovery still applies: the tail
    /// reclaim runs once (without the grace poll) and the read is retried
    /// when it freed a dead writer's leftover.
    pub fn try_recv(&mut self) -> Result<Option<Vec<u8>>> {
        self.refresh_cursors()?;
        if self.closed && self.len == 0 {
            return Err(ChannelError::Closed.into());
        }
        if !self.tail_committed()? {
            if self.try_reclaim_at_tail()? == 0 || !self.tail_committed()? {
                return Ok(None);
            }
        }
        Ok(Some(self.recv_with_seq()?.payload))
    }

    /// Whether the tail slot is COMMITTED — the cheap readability probe
    /// behind `try_recv` (one slot-header read, no poll, no reclaim).
    fn tail_committed(&self) -> Result<bool> {
        let (state, _, _) = self.region.read_header(self.tail)?;
        Ok(state == SlotState::Committed)
    }

    /// Shared prologue of the receive paths (`recv_with_seq`, `recv_view`):
    /// refresh, the closed/empty decision, and the COMMITTED gate with its
    /// crash-leftover reclaim.
    ///
    /// Returns whether a sequence resync is pending (a reclaim or an open
    /// with nothing readable legitimately broke the +1 chain; the caller
    /// adopts the observed sequence instead of enforcing it). On success the
    /// tail slot is COMMITTED and `self.tail` is its ring index.
    fn ensure_tail_readable(&mut self) -> Result<bool> {
        self.refresh_cursors()?;
        if self.closed && self.len == 0 {
            return Err(ChannelError::Closed.into());
        }

        let resync = std::mem::take(&mut self.resync_expected);

        // Gate: only a fully COMMITTED slot is readable. The producer
        // publishes the batch and the head cursor with one flush, so in a
        // healthy run this polls for at most the flush window; a WRITING
        // slot that stays put past the grace period is a crash leftover
        // (see verify C7), which crash recovery reclaims from a provably
        // dead writer before giving up.
        if !self.poll_tail_committed()? {
            if self.try_reclaim_at_tail()? == 0 {
                self.resync_expected = resync;
                return Err(ChannelError::BufferEmpty.into());
            }
            // Retry once without the grace poll: the reclaim may have
            // uncovered a live committed message right behind the dead
            // reservation (multi-producer mode).
            let (state, _, _) = self.region.read_header(self.tail)?;
            if state != SlotState::Committed {
                // Nothing readable yet. Keep the reclaim's resync flag: the
                // next read must adopt the true post-crash sequence.
                return Err(ChannelError::BufferEmpty.into());
            }
        }
        Ok(resync)
    }

    pub fn recv_with_seq(&mut self) -> Result<ReceivedMessage> {
        let resync = self.ensure_tail_readable()?;

        let snapshot = self.region.read_committed(self.tail)?;

        // SPSC: the producer publishes head only after commit, so the tail
        // slot's sequence must be exactly the next expected one — unless a
        // resync was pending, in which case the observed sequence is
        // adopted. MPSC: the head cursor is reserved before the write and
        // the reservation order of messages is not a single writer's order,
        // so per-message sequence contiguity is not enforced — ordering and
        // integrity come from the ring position, the COMMITTED gate, and
        // the checksum instead (see `send_mpsc`).
        if resync {
            self.expected_recv_seq = snapshot.seq;
        } else if !self.config.multi_producer && snapshot.seq != self.expected_recv_seq {
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

    // ── Zero-copy alloc/publish (roadmap §4.2) ──
    //
    // The safety-gate conditions (roadmap §4.3) this milestone satisfies:
    // the payload is born in the shared region (`alloc` reserves the slot
    // and the lease writes into it directly), `publish` seals the buffer
    // and commits it, and the consumer's view pins the slot until released
    // (`pin_floor` keeps the send guard from wrapping onto it, with the
    // lease/seal/pin state machine covered by unit tests and the
    // solver-based invariant checks in `verify/check_invariants.py`).

    /// Reserve one slot for a zero-copy message and return a writable
    /// lease into the shared region.
    ///
    /// Single-producer mode only (an MPSC reservation is cursor-counted at
    /// claim time, so an unpublished reservation would head-of-line block
    /// the whole ring for as long as the buffer is held): `multi_producer`
    /// channels reject `alloc` with `InvalidConfig`. `DropOldest` is
    /// rejected too — its force-release may free a slot a live view is
    /// pinned on; `DropNewest` has nothing to discard and behaves like
    /// `Raise` here. `Block` waits for space as usual.
    ///
    /// The message must fit one slot (`size <= payload_slot_size`): larger
    /// payloads chunk across slots whose bytes are not contiguous in the
    /// region, which a single view cannot express — use `send` for them.
    pub fn alloc(&mut self, size: usize) -> Result<ShmWriteLease>
    where
        S: Send + Sync + 'static,
    {
        if self.closed {
            return Err(ChannelError::Closed.into());
        }
        if self.config.multi_producer {
            return Err(ChannelError::InvalidConfig(
                "zero-copy alloc requires single-producer mode",
            )
            .into());
        }
        if self.config.backpressure == Backpressure::DropOldest {
            return Err(ChannelError::InvalidConfig(
                "zero-copy is incompatible with DropOldest: the drop may free a pinned slot",
            )
            .into());
        }
        if size > self.config.slot_size {
            return Err(ChannelError::MessageTooLarge {
                len: size,
                slot_size: self.config.slot_size,
            }
            .into());
        }
        let frame_len = FRAME_HEADER_LEN
            .checked_add(size)
            .ok_or(ChannelError::InvalidConfig("size overflow"))?;
        self.refresh_cursors()?;
        // DropNewest cannot discard a half-created message, so it degrades
        // to Raise on the alloc path (an error, nothing queued).
        if !self.prepare_send_slots(1)? {
            return Err(ChannelError::BufferFull.into());
        }
        let index = self.head;
        let (state, _, _) = self.region.read_header(index)?;
        if state != SlotState::Free {
            // The guard counts slots from the cursors; the physical slot
            // must agree (a pinned slot with a stale floor, for instance).
            return Err(ChannelError::BufferFull.into());
        }
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        let stamp = self.writer.stamp(self.writer_epoch, 1);
        self.region
            .write_writing_header(index, seq, frame_len, stamp)?;
        self.head = (self.head + 1) % self.config.capacity;
        self.len += 1;
        let access: std::sync::Arc<dyn RegionAccess> = self.region.clone();
        Ok(ShmWriteLease {
            access,
            index,
            seq,
            payload_offset: region_slot_offset(index, self.region.slot_size())
                + ZERO_COPY_PAYLOAD_OFFSET,
            payload_len: size,
            sealed: false,
        })
    }

    /// Seal and commit a zero-copy buffer: checksum the payload where it
    /// lies (no copy out of the region), write the frame header, flip the
    /// slot to COMMITTED, and publish the head cursor. Returns the message
    /// sequence. The lease stays usable for nothing but `seq`/`size`
    /// queries — further writes are rejected (the roadmap's seal rule).
    pub fn publish(&mut self, lease: &mut ShmWriteLease) -> Result<u64> {
        if self.closed {
            return Err(ChannelError::Closed.into());
        }
        if lease.sealed {
            return Err(ChannelError::InvalidConfig("buffer is already published").into());
        }
        let (state, _, _) = self.region.read_header(lease.index)?;
        if state != SlotState::Writing {
            return Err(ChannelError::InvalidConfig("buffer slot is not reserved").into());
        }
        // Checksum in place through the backend's zero-copy read window.
        let checksum =
            self.region
                .storage
                .with_slice(lease.payload_offset, lease.payload_len, checksum32)?;
        let header = encode_frame_header(
            FrameKind::Bytes,
            0,
            lease.seq,
            0,
            0,
            checksum,
            lease.payload_len,
            &[],
        )?;
        let frame_offset = lease.payload_offset - FRAME_HEADER_LEN;
        let frame_len = FRAME_HEADER_LEN + lease.payload_len;
        self.region.storage.write_at(frame_offset, &header)?;
        // Release-fence the frame header, then commit the slot: a reader
        // that observes COMMITTED never sees a stale checksum or header.
        fence(Ordering::Release);
        self.region
            .write_header(lease.index, SlotState::Committed, lease.seq, frame_len)?;
        lease.sealed = true;
        // Publish the batch exactly like `send`: the head cursor store
        // (release) plus one flush over the slot and the cursor.
        self.head_cursor = self.head_cursor.wrapping_add(1);
        self.region.store_head(self.head_cursor)?;
        self.region.storage.flush_range(
            region_slot_offset(lease.index, self.region.slot_size()),
            PERSISTENT_SLOT_HEADER_LEN + FRAME_HEADER_LEN + lease.payload_len,
        )?;
        Ok(lease.seq)
    }

    /// Free an unpublished `alloc` reservation: clear the payload, flip the
    /// slot back to FREE, and rewind the producer's local position. Only
    /// the newest reservation can be unwound — the SPSC head cursor was
    /// never published, so the slot is invisible to consumers.
    pub fn abort(&mut self, lease: ShmWriteLease) -> Result<()> {
        if lease.sealed {
            return Err(ChannelError::InvalidConfig("cannot abort a published buffer").into());
        }
        let last_reserved = (self.head + self.config.capacity - 1) % self.config.capacity;
        if lease.index != last_reserved {
            return Err(ChannelError::InvalidConfig(
                "only the newest unpublished buffer can be aborted",
            )
            .into());
        }
        self.region.force_release_no_flush(lease.index)?;
        self.region.storage.flush_range(
            region_slot_offset(lease.index, self.region.slot_size()),
            PERSISTENT_SLOT_HEADER_LEN,
        )?;
        self.head = lease.index;
        self.len = self.len.saturating_sub(1);
        self.next_seq = lease.seq;
        Ok(())
    }

    /// Receive the next message as a zero-copy view over the shared
    /// region: the slot is pinned (COMMITTED → PINNED with this handle's
    /// consumer identity), the tail cursor advances (logical consume), and
    /// the returned view reads — or partially reads — the payload in place
    /// until released.
    ///
    /// The pin floor records the oldest pinned cursor value, and every
    /// producer's send guard refuses to wrap the ring onto a pinned slot;
    /// release views promptly or the ring fills to a standstill. Rejected
    /// on `DropOldest` channels (a forced drop may free a pinned slot) and
    /// for chunked multi-slot messages (their bytes are not contiguous —
    /// use `recv`).
    pub fn recv_view(&mut self) -> Result<ShmRecvView>
    where
        S: Send + Sync + 'static,
    {
        if self.config.backpressure == Backpressure::DropOldest {
            return Err(ChannelError::InvalidConfig(
                "zero-copy views are incompatible with DropOldest: the drop may free a pinned slot",
            )
            .into());
        }
        let resync = self.ensure_tail_readable()?;
        let index = self.tail;
        let (state, slot_seq, frame_len) = self.region.read_header(index)?;
        debug_assert_eq!(state, SlotState::Committed);
        if frame_len < FRAME_HEADER_LEN {
            return Err(ChannelError::CorruptedMessage.into());
        }
        let frame_offset =
            region_slot_offset(index, self.region.slot_size()) + PERSISTENT_SLOT_HEADER_LEN;
        let header_bytes = self
            .region
            .storage
            .read_at(frame_offset, FRAME_HEADER_LEN)?;
        let (header, _total_len) = decode_frame_header(&header_bytes)?;
        // Zero-copy eligibility: a single-slot, unchunked bytes frame. A
        // larger header means TLV metadata (chunking), which lives in
        // non-contiguous slots.
        if header.header_len as usize != FRAME_HEADER_LEN
            || header.flags != 0
            || header.kind != FrameKind::Bytes
        {
            return Err(ChannelError::InvalidConfig(
                "zero-copy views require single-slot unchunked messages; use recv()",
            )
            .into());
        }
        if header.seq != slot_seq {
            return Err(ChannelError::SequenceMismatch {
                expected: slot_seq,
                actual: header.seq,
            }
            .into());
        }
        if resync {
            self.expected_recv_seq = slot_seq;
        } else if !self.config.multi_producer && slot_seq != self.expected_recv_seq {
            return Err(ChannelError::SequenceMismatch {
                expected: self.expected_recv_seq,
                actual: slot_seq,
            }
            .into());
        }
        let payload_len = header.payload_len as usize;
        if payload_len != frame_len - FRAME_HEADER_LEN {
            return Err(ChannelError::CorruptedMessage.into());
        }
        // Verify the checksum over the payload in place (no copy).
        let checksum = self.region.storage.with_slice(
            frame_offset + FRAME_HEADER_LEN,
            payload_len,
            checksum32,
        )?;
        if checksum != header.checksum {
            return Err(ChannelError::CorruptedMessage.into());
        }
        // Pin: the PINNED header (with this handle's consumer identity,
        // so a crashed consumer's pins are reclaimable) lands first; the
        // pin floor follows. A producer that reads a stale-high floor in
        // between at worst over-commits into a write error — never into a
        // silent overwrite, because the Free-state check backs every
        // physical write.
        let cursor_value = self.tail_cursor;
        let stamp = self.writer.stamp(self.writer_epoch, 1);
        self.region
            .write_pinned_header(index, slot_seq, frame_len, stamp)?;
        self.region.floor_min(cursor_value)?;
        self.advance_tail_keep_pinned()?;
        self.expected_recv_seq = self.expected_recv_seq.wrapping_add(1);
        self.last_received_seq = Some(slot_seq);
        let access: std::sync::Arc<dyn RegionAccess> = self.region.clone();
        Ok(ShmRecvView {
            access,
            index,
            seq: slot_seq,
            payload_offset: frame_offset + FRAME_HEADER_LEN,
            payload_len,
            cursor_value,
            released: false,
        })
    }

    /// Count currently pinned slots (bounded scan; diagnostics for
    /// `stats()["pinned_slots"]`).
    pub fn pinned_slots(&self) -> Result<usize> {
        let mut pinned = 0;
        for index in 0..self.config.capacity {
            if self.region.read_header(index)?.0 == SlotState::Pinned {
                pinned += 1;
            }
        }
        Ok(pinned)
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
    pub fn multi_producer(&self) -> bool {
        self.config.multi_producer
    }

    /// Test and diagnostics aid: write a WRITING header with an arbitrary
    /// writer identity at the producer's next slot, exactly as a writer
    /// killed mid-send would leave the region. Used by the test suites to
    /// simulate a crashed writer deterministically; not part of the
    /// production protocol.
    pub fn debug_leave_zombie(
        &mut self,
        seq: u64,
        pid: u32,
        start_time: u64,
        reserved_slots: u32,
    ) -> Result<()> {
        self.refresh_cursors()?;
        let identity = WriterIdentity {
            pid,
            start_time,
            epoch: 0,
            timestamp_ns: unix_timestamp_ns(),
            reserved_slots,
        };
        self.region
            .write_writing_header(self.head, seq, 1, identity)?;
        self.recovery_bias += 1;
        self.len = (self.len + 1).min(self.config.capacity);
        Ok(())
    }

    fn prepare_send_slots(&mut self, needed_slots: usize) -> Result<bool> {
        if needed_slots > self.config.capacity {
            return Err(ChannelError::MessageTooLarge {
                len: needed_slots,
                slot_size: self.config.capacity,
            }
            .into());
        }
        if self.config.capacity - self.guard_len() >= needed_slots {
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
                while self.config.capacity - self.guard_len() < needed_slots {
                    self.drop_oldest_message()?;
                }
                Ok(true)
            }
        }
    }

    fn wait_for_space(&mut self, needed_slots: usize) -> Result<()> {
        if self.config.capacity - self.guard_len() >= needed_slots {
            return Ok(());
        }
        let Some(timeout) = self.config.timeout else {
            return Err(ChannelError::BufferFull.into());
        };
        self.wait_for_space_until(needed_slots, Instant::now() + timeout)
    }

    /// Wait for space until `deadline`. The caller owns the deadline so that
    /// a retried reservation cannot extend the wait past the configured
    /// timeout (see `reserve_slots`).
    fn wait_for_space_until(&mut self, needed_slots: usize, deadline: Instant) -> Result<()> {
        while Instant::now() < deadline {
            self.refresh_cursors()?;
            if self.config.capacity - self.guard_len() >= needed_slots {
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
    ///
    /// Two cursor regimes exist:
    ///
    /// * head-ahead (normal): `head - tail <= capacity`. The depth is the
    ///   committed prefix plus `recovery_bias` (crash leftovers beyond the
    ///   published head).
    /// * tail-ahead (orphan): `tail - head <= capacity`. The consumer
    ///   consumed COMMITTED slots whose head counts were never published —
    ///   a producer crashed (or is still mid-batch) between the slot
    ///   commits and the head store. Everything readable then lives in the
    ///   bias alone.
    ///
    /// Before the orphan regime existed, a consumer that read such a slot
    /// advanced tail past head and the next refresh rejected the ring as
    /// inconsistent — reachable deterministically after a mid-batch crash
    /// (verify C11).
    fn refresh_cursors(&mut self) -> Result<()> {
        let head_c = self.region.load_head()?;
        let tail_c = self.region.load_tail()?;
        let floor_c = self.region.load_floor()?;
        let cap = self.config.capacity as u64;
        let fwd = head_c.wrapping_sub(tail_c);
        let back = tail_c.wrapping_sub(head_c);
        self.head_cursor = head_c;
        self.tail_cursor = tail_c;
        self.floor_cursor = floor_c;
        self.tail = (tail_c % cap) as usize;
        if fwd <= cap {
            self.len = ((fwd as usize) + self.recovery_bias).min(self.config.capacity);
        } else if back <= cap {
            self.len = self.recovery_bias.min(self.config.capacity);
        } else {
            return Err(ChannelError::InvalidConfig("ring cursors are inconsistent").into());
        }
        Ok(())
    }

    /// Occupied-slot count the **send guard** must respect: the cursor depth
    /// plus every zero-copy view pin between the pin floor and the head.
    ///
    /// Pinned slots are logically consumed (behind the tail cursor), so the
    /// plain depth does not see them — but their physical slots are still
    /// reserved for live views, and a claim that wrapped the ring onto one
    /// would overwrite bytes a consumer is still reading. With
    /// `floor_cursor` ranging over `[tail - capacity, tail]` (or the
    /// "no pins" sentinel, which maps to the tail), the span
    /// `head - floor` counts exactly the slots between the floor and the
    /// head: every pinned slot, plus every released-but-behind-a-pin slot
    /// that is unusable anyway because claims are contiguous from `head`.
    /// Without pins this reduces bit-for-bit to the plain cursor depth.
    ///
    /// Guard theorem (verify Z6): a reservation of `needed` slots claiming
    /// cursors `[head, head + needed)` touches a pinned physical slot iff
    /// `head + needed > floor + capacity` — this count is that condition.
    fn guard_len(&self) -> usize {
        let floor_eff = if self.floor_cursor == PIN_FLOOR_NONE {
            self.tail_cursor
        } else {
            self.floor_cursor.min(self.tail_cursor)
        };
        let pinned_span = self.head_cursor.saturating_sub(floor_eff);
        (pinned_span as usize + self.recovery_bias).min(self.config.capacity)
    }

    /// Rebuild the pin floor from the ring during open recovery: the oldest
    /// still-pinned slot inside the window of consumable cursor values
    /// `[tail - capacity, tail)`, or [`PIN_FLOOR_NONE`] when nothing is
    /// pinned. Ascending cursor order maps one-to-one onto ring positions,
    /// so the first PINNED header encountered is the smallest pinned cursor.
    fn scan_pin_floor(&self, _head_c: u64, tail_c: u64) -> Result<u64> {
        let cap = self.config.capacity as u64;
        let lo = tail_c.saturating_sub(cap);
        for cursor in lo..tail_c {
            let index = (cursor % cap) as usize;
            if self.region.read_header(index)?.0 == SlotState::Pinned {
                return Ok(cursor);
            }
        }
        Ok(PIN_FLOOR_NONE)
    }

    /// One-time recovery scan at open. The shared cursors carry the ring
    /// position; the scan rebuilds the pieces the cursors cannot represent:
    /// the producer's next message sequence (max occupied seq + 1), the
    /// consumer's expected sequence (the seq of the oldest readable slot),
    /// and the producer's write position (first FREE slot after the oldest
    /// readable slot — this also steps over any WRITING slot left by a
    /// crashed producer). `recovery_bias` keeps crash leftovers in the queue
    /// depth so the send guard never over-commits a physically full ring.
    ///
    /// Crash recovery runs first in SPSC mode: WRITING slots whose writer
    /// identity is provably dead are freed before the scan (in SPSC mode
    /// the crashed batch's slots always sit beyond the published head, so
    /// freeing them changes no cursor). MPSC mode reclaims at the tail
    /// instead (see `try_reclaim_at_tail_mpsc`): the dead reservation is
    /// cursor-counted and only the tail can advance past it.
    fn recover_from_storage(&mut self) -> Result<()> {
        if !self.config.multi_producer {
            for index in 0..self.config.capacity {
                if self.region.reclaim_dead_writing(index)? {
                    self.reclaimed_slots += 1;
                    self.resync_expected = true;
                }
            }
        }
        // Dead consumers' zero-copy pins are reclaimable in both modes (the
        // pin is behind the tail cursor, so no cursor changes): a consumer
        // that died holding views would otherwise pin-hole the ring — the
        // floor would block every wrap onto its slots forever.
        for index in 0..self.config.capacity {
            if self.region.reclaim_dead_pinned(index)? {
                self.reclaimed_slots += 1;
            }
        }
        let occupied = self.occupied_slot_headers()?;
        let head_c = self.region.load_head()?;
        let tail_c = self.region.load_tail()?;
        // Rebuild the pin floor from the ring: the oldest still-pinned slot
        // inside the consumable window is the floor; with none, the "no
        // pins" sentinel. The raise is a single CAS from the pre-scan
        // snapshot, so a concurrent pinner's lower floor is never clobbered.
        let floor_before = self.region.load_floor()?;
        let floor = self.scan_pin_floor(head_c, tail_c)?;
        if floor_before != floor {
            self.region.floor_cas(floor_before, floor)?;
        }
        self.floor_cursor = self.region.load_floor()?;
        let cap = self.config.capacity as u64;
        let fwd = head_c.wrapping_sub(tail_c);
        let back = tail_c.wrapping_sub(head_c);
        let cursor_len = if fwd <= cap {
            fwd.min(cap) as usize
        } else if back <= cap {
            0
        } else {
            return Err(ChannelError::InvalidConfig("ring cursors are inconsistent").into());
        };

        self.head_cursor = head_c;
        self.tail_cursor = tail_c;
        let tail_index = (tail_c % cap) as usize;
        self.tail = tail_index;
        self.head = self.infer_head_from_tail(tail_index)?;
        // The scan sees every occupied slot; the cursors only see fully
        // committed batches. The difference is crash leftovers — with one
        // exception: zero-copy pins live behind the tail cursor and are
        // accounted through the pin floor instead (`guard_len`'s pinned
        // span counts `[floor, head)`); biasing them here as well would
        // double-book them and over-shrink the ring. The orphan regime
        // (tail ahead of head) keeps pins in the bias, because their
        // cursors sit beyond the published head where the span cannot
        // reach — over-counting there is merely conservative.
        let pinned_count = occupied
            .iter()
            .filter(|slot| slot.state == SlotState::Pinned)
            .count();
        let bias_pool = if fwd <= cap {
            occupied.len().saturating_sub(pinned_count)
        } else {
            occupied.len()
        };
        self.recovery_bias = bias_pool.saturating_sub(cursor_len);
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
        // restarts at 0 (the current-region convention). The zero is a
        // convention, not knowledge — resync mode makes the first read
        // adopt whatever sequence actually arrives.
        let readable = occupied.iter().any(|s| s.state != SlotState::Writing);
        self.expected_recv_seq = if readable {
            self.region.read_header(self.tail)?.1
        } else {
            0
        };
        self.resync_expected |= !readable;
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

    /// Reclaim a crashed writer's leftovers at the consumer's tail. Called
    /// only after the COMMITTED gate failed its grace poll (never on the
    /// hot path). Returns the number of slots freed (0 = nothing to do).
    ///
    /// The two modes differ in what the cursors count:
    ///
    /// * SPSC — the crashed batch's head was never published, so its
    ///   WRITING slots sit beyond the cursors and are credited through
    ///   `recovery_bias`. The reclaim frees the contiguous WRITING run at
    ///   the tail and retires the matching bias; the tail cursor must NOT
    ///   move (it would overtake the head cursor).
    /// * MPSC — the head cursor is reserved before the write, so the dead
    ///   reservation is cursor-counted. The reclaim frees exactly the
    ///   reservation's slots and advances the shared tail cursor past them
    ///   with a CAS (the same linearization the DropOldest path uses).
    ///
    /// Both variants only ever touch slots whose writer identity is
    /// provably dead (see `reclaim_dead_writing`); a live writer's
    /// in-flight slots classify as `Alive` and are left alone.
    fn try_reclaim_at_tail(&mut self) -> Result<u64> {
        if self.config.multi_producer {
            self.try_reclaim_at_tail_mpsc()
        } else {
            self.try_reclaim_at_tail_spsc()
        }
    }

    fn try_reclaim_at_tail_spsc(&mut self) -> Result<u64> {
        // Only beyond-cursor leftovers are reclaimable here: cursor-counted
        // slots sit in the committed prefix, where C6 proves a healthy run
        // never exposes a WRITING slot — a WRITING slot inside the window
        // would be corruption and is left untouched (freeing it without a
        // cursor advance would strand a FREE slot inside the window).
        let cap = self.config.capacity as u64;
        let fwd = self.head_cursor.wrapping_sub(self.tail_cursor);
        if fwd != 0 && fwd <= cap {
            return Ok(0);
        }
        let mut freed = 0u64;
        let mut index = self.tail;
        for _ in 0..self.config.capacity {
            let (state, _, _) = self.region.read_header(index)?;
            if state != SlotState::Writing {
                break;
            }
            if !self.region.reclaim_dead_writing(index)? {
                break; // alive/unknown writer or a transitional header
            }
            freed += 1;
            index = (index + 1) % self.config.capacity;
        }
        if freed == 0 {
            return Ok(0);
        }
        self.recovery_bias = self.recovery_bias.saturating_sub(freed as usize);
        self.reclaimed_slots += freed;
        self.resync_expected = true;
        self.len = self.len.saturating_sub(freed as usize);
        Ok(freed)
    }

    fn try_reclaim_at_tail_mpsc(&mut self) -> Result<u64> {
        // The reclaim extent comes from the tail slot's own reservation
        // record. Everything is verified before a single slot is touched:
        // a partial free without the cursor advance would strand FREE slots
        // inside the cursor window, which the gate can never pass.
        let head = self.head_cursor;
        let tail = self.tail_cursor;
        let cap = self.config.capacity as u64;
        let window = head.wrapping_sub(tail);
        if window == 0 || window > cap {
            return Ok(0); // nothing cursor-counted at the tail
        }
        let first = self.region.read_header_full(self.tail)?;
        if first.state != SlotState::Writing {
            return Ok(0);
        }
        let Some(identity) = first.identity else {
            return Ok(0); // no published identity: never auto-reclaim
        };
        if process_liveness(ProcessIdentity {
            pid: identity.pid,
            start_time: identity.start_time,
        }) != ProcessLiveness::Gone
        {
            return Ok(0);
        }
        let extent = u64::from(identity.reserved_slots);
        if extent == 0 || extent > window {
            // An unknown or out-of-window extent cannot be reclaimed
            // safely: advancing the tail could skip live messages.
            return Ok(0);
        }
        // Verify the whole extent: only slots of the same dead writer
        // (WRITING) or never-written reservation slots (FREE) may be
        // crossed. Anything else means the extent record is untrustworthy.
        let mut index = self.tail;
        for _ in 0..extent {
            let full = self.region.read_header_full(index)?;
            match full.state {
                SlotState::Free => {}
                SlotState::Writing => match full.identity {
                    Some(sid)
                        if sid.pid == identity.pid && sid.start_time == identity.start_time => {}
                    _ => return Ok(0),
                },
                _ => return Ok(0),
            }
            index = (index + 1) % self.config.capacity;
        }
        // Free the extent, then advance the tail with a CAS. While the
        // slots stay inside [tail, head) no producer may claim them (the
        // reservation guard runs on the CAS), so the frees are race-free;
        // a competing reclaimer's CAS simply wins and this one re-evaluates.
        let mut index = self.tail;
        for _ in 0..extent {
            self.region.force_release_no_flush(index)?;
            self.region.flush_release(index)?;
            index = (index + 1) % self.config.capacity;
        }
        let (actual, ok) = self.region.compare_exchange_tail(tail, tail + extent)?;
        if !ok {
            return Ok(0); // another handle advanced the tail first
        }
        let _ = actual;
        self.tail_cursor = tail + extent;
        self.tail = ((tail + extent) % cap) as usize;
        self.reclaimed_slots += extent;
        self.resync_expected = true;
        self.len = self.len.saturating_sub(extent as usize);
        self.region.flush_header()?;
        Ok(extent)
    }

    /// Explicit owner cleanup (roadmap §6: "owner 明确执行 cleanup"):
    /// repeatedly reclaim crashed writers' leftovers reachable from the
    /// tail. Returns the number of slots freed. Leftovers deeper in the
    /// ring are reclaimed by the same rule when the tail reaches them.
    pub fn reclaim_stale(&mut self) -> Result<u64> {
        self.refresh_cursors()?;
        if self.config.multi_producer {
            // MPSC: only the cursor-counted reservation at the tail can be
            // reclaimed (the tail CAS is the only way past it).
            let mut total = 0u64;
            for _ in 0..self.config.capacity {
                let freed = self.try_reclaim_at_tail_mpsc()?;
                if freed == 0 {
                    break;
                }
                total += freed;
                self.refresh_cursors()?;
            }
            // Dead consumers' pins sit behind the tail and are freed by
            // identity, like everywhere else; the floor follows.
            for index in 0..self.config.capacity {
                if self.region.reclaim_dead_pinned(index)? {
                    total += 1;
                }
            }
            if total > 0 {
                self.reclaimed_slots += total;
                let tail_c = self.region.load_tail()?;
                let floor_before = self.region.load_floor()?;
                let floor = self.scan_pin_floor(0, tail_c)?;
                if floor_before != floor {
                    self.region.floor_cas(floor_before, floor)?;
                }
                self.floor_cursor = self.region.load_floor()?;
            }
            return Ok(total);
        }
        // SPSC: free every dead writer's WRITING slot outside the cursor
        // window and recompute the bias from storage. The crashed batch's
        // slots sit beyond the published head, so no cursor moves.
        let head_c = self.region.load_head()?;
        let tail_c = self.region.load_tail()?;
        let cap = self.config.capacity as u64;
        let mut covered = vec![false; self.config.capacity];
        for count in tail_c..head_c {
            covered[(count % cap) as usize] = true;
        }
        let mut total = 0u64;
        for index in 0..self.config.capacity {
            if covered[index] {
                continue;
            }
            if self.region.reclaim_dead_writing(index)? {
                total += 1;
            }
            if self.region.reclaim_dead_pinned(index)? {
                total += 1;
            }
        }
        if total > 0 {
            self.reclaimed_slots += total;
            self.resync_expected = true;
            self.recovery_bias = self.recompute_spsc_bias()?;
            self.refresh_cursors()?;
            // Pins freed above raise the floor (fewer reservations); move
            // the shared word accordingly so the guard un-shrinks too.
            let head_c = self.region.load_head()?;
            let tail_c = self.region.load_tail()?;
            let floor_before = self.region.load_floor()?;
            let floor = self.scan_pin_floor(head_c, tail_c)?;
            if floor_before != floor {
                self.region.floor_cas(floor_before, floor)?;
            }
            self.floor_cursor = self.region.load_floor()?;
        }
        Ok(total)
    }

    /// Recompute the SPSC recovery bias from storage after a reclaim: the
    /// occupied slot count minus the cursor-counted committed prefix.
    fn recompute_spsc_bias(&mut self) -> Result<usize> {
        let head_c = self.region.load_head()?;
        let tail_c = self.region.load_tail()?;
        let cap = self.config.capacity as u64;
        let fwd = head_c.wrapping_sub(tail_c);
        let back = tail_c.wrapping_sub(head_c);
        let cursor_len = if fwd <= cap {
            fwd.min(cap) as usize
        } else if back <= cap {
            0
        } else {
            return Err(ChannelError::InvalidConfig("ring cursors are inconsistent").into());
        };
        let occupied = self.occupied_slot_headers()?.len();
        Ok(occupied.saturating_sub(cursor_len))
    }

    /// Slots this handle has reclaimed from provably dead writers.
    pub fn reclaimed_slots(&self) -> u64 {
        self.reclaimed_slots
    }

    fn advance_tail_after_read(&mut self) -> Result<()> {
        self.advance_tail_impl(true)
    }

    /// Zero-copy consume: advance the tail cursor but keep the slot PINNED.
    /// The view returned to the caller owns the physical release; until
    /// then the send guard keeps the ring from wrapping onto the slot via
    /// the pin floor.
    fn advance_tail_keep_pinned(&mut self) -> Result<()> {
        self.advance_tail_impl(false)
    }

    fn advance_tail_impl(&mut self, release_slot: bool) -> Result<()> {
        // Was the slot just consumed counted by the cursors? head strictly
        // ahead means yes; head == tail (or already tail-ahead) means the
        // slot was an orphan — occupied beyond the published head, credited
        // to the recovery bias instead.
        let cap = self.config.capacity as u64;
        let fwd = self.head_cursor.wrapping_sub(self.tail_cursor);
        let orphan = fwd == 0 || fwd > cap;
        let index = self.tail;
        if release_slot {
            self.region.release_pinned_no_flush(index)?;
        }
        self.tail = (self.tail + 1) % self.config.capacity;
        // Publish the FREE/PINNED header and the advanced tail cursor (two
        // page-local flushes; the cursor lives in the first page).
        self.tail_cursor = self.tail_cursor.wrapping_add(1);
        self.region.fetch_max_tail(self.tail_cursor)?;
        // Publish the header and the advanced tail cursor.
        self.region.flush_release(index)?;
        // `len` is re-derived from the cursors at the start of every
        // operation, so it can undercount while a producer is mid-write;
        // never go below zero (the next refresh recomputes the true depth).
        self.len = self.len.saturating_sub(1);
        if orphan {
            // The consumed slot was never cursor-counted: retire its bias
            // credit so the send guard's view stays exact.
            self.recovery_bias = self.recovery_bias.saturating_sub(1);
        }
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
        if self.config.multi_producer {
            return self.drop_oldest_message_mpsc();
        }
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
            let cap = self.config.capacity as u64;
            let fwd = self.head_cursor.wrapping_sub(self.tail_cursor);
            let orphan = fwd == 0 || fwd > cap;
            let index = self.tail;
            // Force-release: the message was validated COMMITTED by the
            // peek, and the consumer may have pinned the slot in between —
            // the drop still discards it (see `force_release_no_flush`).
            self.region.force_release_no_flush(index)?;
            self.tail = (self.tail + 1) % self.config.capacity;
            self.tail_cursor = self.tail_cursor.wrapping_add(1);
            self.region.fetch_max_tail(self.tail_cursor)?;
            self.region.flush_release(index)?;
            self.len = self.len.saturating_sub(1);
            if orphan {
                // The dropped slot was occupied beyond the published head
                // (orphan regime): retire its bias credit.
                self.recovery_bias = self.recovery_bias.saturating_sub(1);
            }
        }
        self.expected_recv_seq = self.expected_recv_seq.wrapping_add(1);
        Ok(())
    }

    /// MPSC DropOldest. The tail may be another producer's in-flight
    /// reservation, so the bounded COMMITTED gate runs first; and the tail
    /// advance is a CAS over the dropped message's slot count, so concurrent
    /// droppers never drop the same message twice. A lost CAS just means
    /// another dropper made progress first — the caller's guard loop
    /// re-evaluates.
    ///
    /// The peek/decode is retried (unbounded) because a concurrent dropper
    /// can tear the tail slot mid-release: every competing drop is bounded
    /// work, so the retry loop always reaches a stable tail. A WRITING tail
    /// is waited out rather than refused (see the loop), so the only exit
    /// without progress is a writer that neither commits nor dies within the
    /// bound.
    fn drop_oldest_message_mpsc(&mut self) -> Result<()> {
        const DROP_WAIT: Duration = Duration::from_secs(30);
        let deadline = Instant::now() + DROP_WAIT;
        loop {
            self.refresh_cursors()?;
            if self.len == 0 {
                return Ok(());
            }
            let tail_count = self.tail_cursor;
            if !self.poll_tail_committed()? {
                // A WRITING tail is either another producer's in-flight
                // reservation — which commits shortly, so the drop must *wait
                // for it* — or a crashed writer's leftover, which the reclaim
                // takes. Refusing as soon as the cursors stand still conflated
                // the two: a healthy send on a DropOldest channel failed with
                // `BufferFull` whenever it raced another producer's write
                // (reproduced 2/20 on Windows), contradicting the documented
                // "drop strategies keep `send` a successful call". So reclaim
                // the dead case, wait out the live one, and keep a wall-clock
                // bound for a writer that never finishes (a *yield* budget is
                // scheduler-dependent and fires on healthy peers under load).
                if self.try_reclaim_at_tail()? > 0 {
                    continue;
                }
                self.refresh_cursors()?;
                if self.tail_cursor == tail_count {
                    if Instant::now() >= deadline {
                        return Err(ChannelError::BufferFull.into());
                    }
                    thread::yield_now();
                }
                continue;
            }
            let snapshot = match self.region.peek_committed(self.tail) {
                Ok(snapshot) => snapshot,
                Err(_) => continue, // released while peeking; re-evaluate
            };
            let frame = match decode_frame(&snapshot.data) {
                Ok(frame) => frame,
                Err(_) => continue, // concurrent release tore the frame
            };
            let slots_to_drop = if is_chunked(&frame) {
                let info = match chunk_info(&frame) {
                    Ok(info) => info,
                    Err(_) => continue, // torn chunk metadata; re-evaluate
                };
                if info.index != 0 {
                    return Err(ChannelError::CorruptedMessage.into());
                }
                info.total
            } else {
                1
            };
            let (_, ok) = self
                .region
                .compare_exchange_tail(tail_count, tail_count + slots_to_drop as u64)?;
            if !ok {
                return Ok(());
            }
            for _ in 0..slots_to_drop {
                let index = self.tail;
                // Force-release: the winning CAS already linearized the
                // discard, so the slot is freed even when the consumer
                // pinned it between the peek and the CAS.
                self.region.force_release_no_flush(index)?;
                self.tail = (self.tail + 1) % self.config.capacity;
                self.tail_cursor = self.tail_cursor.wrapping_add(1);
                self.region.flush_release(index)?;
                self.len = self.len.saturating_sub(1);
            }
            self.expected_recv_seq = self.expected_recv_seq.wrapping_add(1);
            return Ok(());
        }
    }
}

impl<S: SlotRegionIo> PersistentShmChannel<S> {
    /// Create the region if it does not exist yet, otherwise open it and
    /// recover the current state. Used for named shared-memory channels
    /// where the first caller initialises the region. The storage backend
    /// decides the create-or-open semantics (the OS guarantees exactly one
    /// creator for named sections; file backends use create-if-missing).
    pub fn create_or_open(path: impl AsRef<Path>, config: SpscConfig) -> Result<Self> {
        config.validate()?;
        let region_slot_size = persistent_region_slot_size(&config)?;
        let total_len = PersistentSlotRegion::<S>::total_len(config.capacity, region_slot_size)?;
        let (storage, created) =
            S::create_or_open(&path, config.capacity, region_slot_size, total_len)?;
        let region = std::sync::Arc::new(PersistentSlotRegion::<S>::from_storage(storage)?);
        if created {
            // Freshly created: initialise all slots as FREE and publish the
            // region magic last.
            Self::new_initialized(region, config)
        } else {
            // The region already exists: wait for the creator's magic, then
            // recover.
            region.wait_initialized(region_magic(config.multi_producer))?;
            Self::opened(region, config)
        }
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
        process, region_magic, region_slot_offset, shm_region_path, FileSlotStorage,
        FixedSlotRegion, FixedSlotStorage, MemorySlotStorage, MmapSlotStorage,
        PersistentShmChannel, PersistentSlotRegion, ShmSpscChannel, SlotRegionIo, SlotState,
        WriterIdentity, PIN_FLOOR_NONE, REGION_HEADER_LEN, ZERO_COPY_PAYLOAD_OFFSET,
    };
    use dsline_core::error::{ChannelError, DslineError};
    use dsline_core::{
        decode_frame, encode_frame, Backpressure, FrameKind, SpscConfig, CHUNK_METADATA_LEN,
        FRAME_HEADER_LEN,
    };
    use std::fs::{self, OpenOptions};
    use std::io::{Seek as _, Write as _};
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
            multi_producer: false,
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
            multi_producer: false,
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
            multi_producer: false,
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
            multi_producer: false,
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
            multi_producer: false,
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
            multi_producer: false,
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
            multi_producer: false,
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
            multi_producer: false,
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
            multi_producer: false,
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
                multi_producer: false,
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
    fn try_recv_polls_without_raising_or_consuming() {
        let path = test_file_path("persistent-try-recv");
        let config = SpscConfig {
            capacity: 4,
            slot_size: 32,
            backpressure: Backpressure::Raise,
            timeout: None,
            multi_producer: false,
        };
        let mut producer = PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone())
            .expect("producer handle");
        let mut consumer =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config).expect("consumer handle");

        // An empty ring reports nothing — twice, without disturbing state.
        assert_eq!(consumer.try_recv().expect("poll empty"), None);
        assert_eq!(consumer.try_recv().expect("poll empty again"), None);

        producer.send(b"one").expect("send one");
        assert_eq!(
            consumer.try_recv().expect("poll one"),
            Some(b"one".to_vec())
        );
        assert_eq!(consumer.try_recv().expect("poll drained"), None);

        // A non-blocking read advances the same sequence state as `recv`:
        // the next message is seq 1 and the strict chain still holds.
        producer.send(b"two").expect("send two");
        assert_eq!(consumer.recv_with_seq().expect("recv two").seq, 1);

        // Closed and empty is a distinct state, exactly as in `recv`.
        consumer.close();
        assert!(matches!(
            consumer.try_recv(),
            Err(DslineError::Channel(ChannelError::Closed))
        ));

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
                multi_producer: false,
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
            multi_producer: false,
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
            multi_producer: false,
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
            multi_producer: false,
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
            .write_committed(0, 0, &frame, None)
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

    // ── crash recovery tests (writer identity, roadmap §6) ──

    // The reclaim tests below need a platform that can prove a writer is
    // gone: only Windows and Linux implement the probe (`process.rs`), and
    // every other platform answers `Unknown`, which the protocol reads as
    // "never reclaim" (roadmap §6.2). They are gated rather than deleted so
    // the platforms without a probe still run the refusal-side tests
    // (`open_keeps_a_live_writer_zombie`,
    // `open_never_reclaims_an_identityless_zombie`,
    // `mpsc_reclaim_refuses_an_extent_crossing_a_committed_slot`), which hold
    // for `Unknown` too.

    /// Spawn a child that exits immediately and reap it: the returned PID
    /// is afterwards a provably dead identity.
    #[cfg(any(windows, target_os = "linux"))]
    fn spawn_dead_identity() -> (u32, u64) {
        #[cfg(windows)]
        let mut child = std::process::Command::new("cmd")
            .args(["/c", "exit"])
            .spawn()
            .expect("spawn probe child");
        #[cfg(not(windows))]
        let mut child = std::process::Command::new("true")
            .spawn()
            .expect("spawn probe child");
        let pid = child.id();
        child.wait().expect("reap probe child");
        (pid, 0)
    }

    #[test]
    fn process_identity_of_a_live_self_is_alive() {
        let identity = process::current_process_identity();
        assert_eq!(
            process::process_liveness(identity),
            process::ProcessLiveness::Alive
        );
    }

    #[test]
    #[cfg(any(windows, target_os = "linux"))]
    fn open_reclaims_dead_writer_zombie_and_reuses_the_slot() {
        // A producer that died mid-write with its identity recorded leaves
        // a WRITING slot. A later open must verify the identity is dead,
        // free the slot, and continue the ring THROUGH the freed slot
        // (space is reused, sequences continue from the live messages).
        let path = test_file_path("crash-reclaim-at-open");
        let config = persistent_config(4, 64);
        let mut ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone()).expect("create");
        ch.send(b"one").expect("send");
        let (dead_pid, dead_start) = spawn_dead_identity();
        ch.debug_leave_zombie(1, dead_pid, dead_start, 1)
            .expect("zombie");
        drop(ch);

        let mut recovered =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config).expect("reopen");
        assert_eq!(recovered.reclaimed_slots(), 1, "the zombie is reclaimed");
        assert_eq!(recovered.len(), 1, "only the committed message counts");
        assert_eq!(recovered.next_sequence(), 1, "continue after 'one'");

        // The recovered producer writes into the freed zombie slot.
        recovered.send(b"two").expect("send into reclaimed slot");
        assert_eq!(recovered.region.load_head().expect("head"), 2);
        assert_eq!(recovered.recv_with_seq().expect("recv one").payload, b"one");
        assert_eq!(recovered.recv_with_seq().expect("recv two").payload, b"two");

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn open_keeps_a_live_writer_zombie() {
        // A WRITING slot whose writer identity is still alive (the current
        // process) must never be reclaimed — the conservative roadmap rule.
        let path = test_file_path("crash-keep-live-zombie");
        let config = persistent_config(4, 64);
        let mut ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone()).expect("create");
        ch.send(b"one").expect("send");
        let identity = process::current_process_identity();
        ch.debug_leave_zombie(1, identity.pid, identity.start_time, 1)
            .expect("zombie");
        drop(ch);

        let mut recovered =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config).expect("reopen");
        assert_eq!(recovered.reclaimed_slots(), 0, "live writer: no reclaim");
        assert_eq!(recovered.len(), 2, "the zombie still counts in the depth");
        assert_eq!(recovered.next_sequence(), 2);
        // The write position steps over the zombie as before.
        recovered.send(b"two").expect("send past zombie");
        assert_eq!(recovered.region.load_head().expect("head"), 2);
        assert_eq!(recovered.recv_with_seq().expect("recv one").payload, b"one");
        assert!(matches!(
            recovered.recv(),
            Err(DslineError::Channel(ChannelError::BufferEmpty))
        ));

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn open_never_reclaims_an_identityless_zombie() {
        // A WRITING header without the identity magic (older build or a
        // torn write) must not be auto-reclaimed.
        let path = test_file_path("crash-keep-identityless");
        let config = persistent_config(4, 64);
        let ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone()).expect("create");
        ch.region
            .write_header(0, SlotState::Writing, 0, 10)
            .expect("legacy zombie header");
        drop(ch);

        let mut other =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config).expect("reopen");
        assert_eq!(other.reclaimed_slots(), 0);
        assert_eq!(other.len(), 1, "the zombie counts in the depth");

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    #[cfg(any(windows, target_os = "linux"))]
    fn consumer_reclaims_dead_writer_at_the_gate() {
        // The consumer handle opened while the ring was healthy, then the
        // producer crashed mid-write. The recv gate's grace poll expires,
        // the dead writer's slot is reclaimed, and the next message (from a
        // recovered producer) is delivered with a resynced sequence.
        let path = test_file_path("crash-gate-reclaim");
        let config = persistent_config(4, 64);
        let mut producer =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone()).expect("create");
        let mut consumer =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config.clone()).expect("open");
        producer.send(b"one").expect("send");
        let (dead_pid, dead_start) = spawn_dead_identity();
        producer
            .debug_leave_zombie(1, dead_pid, dead_start, 1)
            .expect("zombie");
        drop(producer);

        assert_eq!(consumer.recv_with_seq().expect("recv one").payload, b"one");
        // The gate hits the zombie, reclaims it, and reports an empty ring.
        assert!(matches!(
            consumer.recv(),
            Err(DslineError::Channel(ChannelError::BufferEmpty))
        ));
        assert_eq!(consumer.reclaimed_slots(), 1);

        // A recovered producer reuses the freed slot; the consumer adopts
        // its sequence through the resync rule.
        let mut recovered =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config).expect("reopen");
        // The crashed writer's sequence was abandoned with its slots; the
        // recovered producer restarts at 0 and the consumer resyncs.
        assert_eq!(recovered.next_sequence(), 0);
        recovered.send(b"two").expect("send");
        assert_eq!(consumer.recv_with_seq().expect("recv two").payload, b"two");

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    #[cfg(any(windows, target_os = "linux"))]
    fn mpsc_gate_reclaims_a_dead_reservation_extent() {
        // MPSC: the head cursor is reserved before the write, so a crashed
        // producer's reservation is cursor-counted. The consumer's gate
        // reclaim frees exactly the recorded extent and CAS-advances the
        // tail past it, uncovering the live message behind it.
        let path = test_file_path("crash-mpsc-extent");
        let mut config = persistent_config(8, 64);
        config.multi_producer = true;
        let mut producer_a =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone()).expect("create");
        let mut consumer =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config.clone()).expect("open");
        producer_a.send(b"first").expect("send");

        // Producer A "reserves" two more slots (head cursor 1 -> 3) and
        // dies after marking them WRITING with its (dead) identity.
        let (dead_pid, dead_start) = spawn_dead_identity();
        let identity = crate::WriterIdentity {
            pid: dead_pid,
            start_time: dead_start,
            epoch: 7,
            timestamp_ns: 0,
            reserved_slots: 2,
        };
        producer_a.region.store_head(3).expect("reserve head");
        producer_a
            .region
            .write_writing_header(1, 1, 10, identity)
            .expect("zombie slot 1");
        producer_a
            .region
            .write_writing_header(2, 1, 10, identity)
            .expect("zombie slot 2");
        drop(producer_a);

        assert_eq!(
            consumer.recv_with_seq().expect("recv first").payload,
            b"first"
        );
        // The gate hits the dead reservation at slot 1...
        assert!(matches!(
            consumer.recv(),
            Err(DslineError::Channel(ChannelError::BufferEmpty))
        ));
        assert_eq!(consumer.reclaimed_slots(), 2, "both reserved slots freed");
        assert_eq!(
            consumer.region.load_tail().expect("tail"),
            3,
            "the tail CAS advanced past the reservation"
        );

        // A fresh producer continues the ring; the live message behind the
        // dead reservation is delivered with the resynced sequence.
        let mut producer_b =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config).expect("reopen");
        // The dead reservation's sequence was abandoned with its slots.
        assert_eq!(producer_b.next_sequence(), 0);
        producer_b.send(b"second").expect("send");
        assert_eq!(
            consumer.recv_with_seq().expect("recv second").payload,
            b"second"
        );

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn mpsc_reclaim_refuses_an_extent_crossing_a_committed_slot() {
        // Defensive stop: if the recorded extent covers a slot that is not
        // the dead writer's (here: COMMITTED), the reclaim must abort with
        // the cursors untouched.
        let path = test_file_path("crash-mpsc-refuse");
        let mut config = persistent_config(8, 64);
        config.multi_producer = true;
        let mut producer =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone()).expect("create");
        let mut consumer =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config.clone()).expect("open");

        let (dead_pid, dead_start) = spawn_dead_identity();
        let identity = crate::WriterIdentity {
            pid: dead_pid,
            start_time: dead_start,
            epoch: 7,
            timestamp_ns: 0,
            reserved_slots: 3,
        };
        producer.region.store_head(3).expect("reserve head");
        producer
            .region
            .write_writing_header(0, 0, 10, identity)
            .expect("zombie slot 0");
        producer
            .region
            .write_writing_header(1, 0, 10, identity)
            .expect("zombie slot 1");
        // Slot 2 holds a COMMITTED frame inside the claimed extent — a
        // state a truthful reservation can never produce.
        let frame = encode_frame(FrameKind::Bytes, 5, 0, 0, 0, &[], b"x").expect("encode");
        producer
            .region
            .write_committed(2, 5, &frame, None)
            .expect("committed inside extent");
        drop(producer);

        assert!(matches!(
            consumer.recv(),
            Err(DslineError::Channel(ChannelError::BufferEmpty))
        ));
        assert_eq!(consumer.reclaimed_slots(), 0, "nothing reclaimed");
        assert_eq!(consumer.region.load_tail().expect("tail"), 0);
        assert_eq!(consumer.len(), 3);

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    #[cfg(any(windows, target_os = "linux"))]
    fn reclaim_stale_reports_the_freed_slot_count() {
        // The explicit cleanup entry point reclaims reachable leftovers and
        // is a no-op on a healthy ring.
        let path = test_file_path("crash-reclaim-stale");
        let config = persistent_config(4, 64);
        let mut ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone()).expect("create");
        assert_eq!(ch.reclaim_stale().expect("healthy noop"), 0);
        ch.send(b"one").expect("send");
        let (dead_pid, dead_start) = spawn_dead_identity();
        ch.debug_leave_zombie(1, dead_pid, dead_start, 1)
            .expect("zombie");
        assert_eq!(ch.reclaim_stale().expect("reclaim"), 1);
        assert_eq!(ch.reclaim_stale().expect("idempotent"), 0);
        assert_eq!(ch.reclaimed_slots(), 1);

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn region_format_word_is_verified_at_open() {
        // A region written with a different layout version must be
        // rejected, never misinterpreted.
        let path = test_file_path("crash-format-version");
        let config = persistent_config(4, 64);
        let ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone()).expect("create");
        drop(ch);
        // Corrupt the format word through a second mapping: std::fs writes
        // are not coherent with mapped views on Windows.
        let region_slot = super::persistent_region_slot_size(&config).expect("region slot");
        let total =
            PersistentSlotRegion::<MmapSlotStorage>::total_len(config.capacity, region_slot)
                .expect("total");
        {
            let storage =
                <MmapSlotStorage as SlotRegionIo>::open(&path, config.capacity, region_slot, total)
                    .expect("storage");
            storage
                .write_at(super::FORMAT_OFFSET, &9u32.to_le_bytes())
                .expect("corrupt format");
        }
        assert!(matches!(
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config),
            Err(DslineError::Channel(ChannelError::InvalidConfig(_)))
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

    // ── named paging-file section backend tests (Windows) ──

    #[cfg(windows)]
    use super::SectionShmSlotStorage;

    #[cfg(windows)]
    fn section_config() -> SpscConfig {
        // Capacity 8: an 80-byte payload spans 4 slots (20 payload bytes per
        // slot at slot_size 64), so "one" + chunked + "two" needs 6 slots.
        persistent_config(8, 64)
    }

    #[test]
    #[cfg(windows)]
    fn section_storage_writes_reads_and_clears() {
        let path = test_file_path("section-basic");
        let storage = <SectionShmSlotStorage as SlotRegionIo>::create(
            &path,
            4,
            127,
            REGION_HEADER_LEN + 4 * 127,
        )
        .expect("storage");

        storage.write_at(21, b"hello").expect("write");
        assert_eq!(storage.read_at(21, 5).expect("read"), b"hello");
        storage.clear_at(21, 5).expect("clear");
        assert_eq!(storage.read_at(21, 5).expect("read cleared"), &[0; 5]);
        assert!(!path.exists(), "section backend leaves no backing file");
    }

    #[test]
    #[cfg(windows)]
    fn section_storage_create_or_open_reports_exactly_one_creator() {
        let path = test_file_path("section-created");
        let (first, created1) = <SectionShmSlotStorage as SlotRegionIo>::create_or_open(
            &path,
            4,
            127,
            REGION_HEADER_LEN + 4 * 127,
        )
        .expect("create");
        let (second, created2) = <SectionShmSlotStorage as SlotRegionIo>::create_or_open(
            &path,
            4,
            127,
            REGION_HEADER_LEN + 4 * 127,
        )
        .expect("open");

        assert!(created1, "the first caller creates the section");
        assert!(!created2, "the second caller opens the existing section");
        first.write_at(21, b"shared").expect("write");
        assert_eq!(second.read_at(21, 6).expect("read"), b"shared");

        drop(first);
        drop(second);
    }

    #[test]
    #[cfg(windows)]
    fn section_storage_open_without_create_fails() {
        let path = test_file_path("section-missing");
        assert!(matches!(
            <SectionShmSlotStorage as SlotRegionIo>::open(
                &path,
                4,
                127,
                REGION_HEADER_LEN + 4 * 127
            ),
            Err(DslineError::Channel(ChannelError::StorageIo(_)))
        ));
    }

    #[test]
    #[cfg(windows)]
    fn section_channels_exchange_messages_through_one_section() {
        let path = test_file_path("section-channels");
        let cfg = section_config();
        let mut producer =
            PersistentShmChannel::<SectionShmSlotStorage>::create_or_open(&path, cfg.clone())
                .expect("producer handle");
        let mut consumer =
            PersistentShmChannel::<SectionShmSlotStorage>::create_or_open(&path, cfg.clone())
                .expect("consumer handle");
        let large = vec![b'z'; 80]; // 4-slot chunked message

        producer.send(b"one").expect("send one");
        producer.send(&large).expect("send chunked");
        producer.send(b"two").expect("send two");

        assert_eq!(consumer.recv_with_seq().expect("recv one").payload, b"one");
        let chunked = consumer.recv_with_seq().expect("recv chunked");
        assert_eq!(chunked.seq, 1);
        assert_eq!(chunked.payload, large);
        assert_eq!(consumer.recv_with_seq().expect("recv two").payload, b"two");
    }

    #[test]
    #[cfg(windows)]
    fn section_region_open_waits_for_the_initialization_magic() {
        // A storage handle exists but the initializer never ran: the
        // channel-level open must poll for the REGION_MAGIC and fail
        // cleanly when it never appears (a creator that died mid-init).
        let path = test_file_path("section-no-magic");
        let storage = <SectionShmSlotStorage as SlotRegionIo>::create_or_open(
            &path,
            4,
            127,
            REGION_HEADER_LEN + 4 * 127,
        )
        .expect("storage without init");
        drop(storage);

        // Recreate the section, then open a channel over it without the
        // initialization pass: the magic stays zero and the open fails.
        let (storage, _created) = <SectionShmSlotStorage as SlotRegionIo>::create_or_open(
            &path,
            4,
            127,
            REGION_HEADER_LEN + 4 * 127,
        )
        .expect("storage again");
        let region =
            PersistentSlotRegion::<SectionShmSlotStorage>::from_storage(storage).expect("region");
        assert!(matches!(
            region.wait_initialized(region_magic(false)),
            Err(DslineError::Channel(ChannelError::InvalidConfig(_)))
        ));
    }

    #[test]
    #[cfg(windows)]
    fn section_create_or_open_initialization_is_skipped_on_reopen() {
        // The second handle must open the existing section (created=false),
        // so the channel's init pass does not run over live data: a message
        // sent through the first handle survives the second opener.
        let path = test_file_path("section-reopen");
        let cfg = section_config();
        let mut first =
            PersistentShmChannel::<SectionShmSlotStorage>::create_or_open(&path, cfg.clone())
                .expect("first");
        first.send(b"hello").expect("send");

        let mut second = PersistentShmChannel::<SectionShmSlotStorage>::create_or_open(&path, cfg)
            .expect("second");
        assert_eq!(second.recv_with_seq().expect("recv").payload, b"hello");
    }

    #[test]
    #[cfg(windows)]
    fn section_lifetime_dies_with_the_last_handle() {
        // The section is destroyed when the last handle closes: a fresh
        // create_or_open afterwards starts from an empty ring (unlike the
        // file backend, whose data survives on disk).
        let path = test_file_path("section-lifetime");
        let cfg = section_config();
        {
            let mut ch =
                PersistentShmChannel::<SectionShmSlotStorage>::create_or_open(&path, cfg.clone())
                    .expect("first life");
            ch.send(b"gone").expect("send");
        }
        let reopened = PersistentShmChannel::<SectionShmSlotStorage>::create_or_open(&path, cfg)
            .expect("second life");
        assert!(reopened.is_empty(), "the section died with its last handle");
    }

    // ── multi-producer (MPSC) tests ──

    fn mpsc_config(capacity: usize) -> SpscConfig {
        SpscConfig {
            capacity,
            slot_size: 64,
            backpressure: Backpressure::Block,
            // Generous on purpose: the suite runs in parallel, and a CI runner
            // has 2-4 cores for it. A tight timeout here measures the runner's
            // scheduler, not the channel (a `Block` send that times out is a
            // legitimate failure, so the budget must not be tight).
            timeout: Some(Duration::from_secs(60)),
            multi_producer: true,
        }
    }

    #[test]
    fn mpsc_mode_mismatch_is_rejected_at_open() {
        // The channel mode is baked into the region magic at creation;
        // opening an SPSC region as MPSC fails fast instead of
        // misinterpreting the ring.
        let path = test_file_path("mpsc-mode");
        PersistentShmChannel::<MmapSlotStorage>::create(&path, persistent_config(4, 64))
            .expect("spsc region");
        assert!(matches!(
            PersistentShmChannel::<MmapSlotStorage>::open(&path, mpsc_config(4)),
            Err(DslineError::Channel(ChannelError::InvalidConfig(_)))
        ));
        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn mpsc_two_producer_threads_deliver_every_message_exactly_once() {
        let path = test_file_path("mpsc-threads");
        let config = mpsc_config(8);
        const PER_PRODUCER: usize = 200;
        let mut producer_a =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone()).expect("create");
        let mut producer_b =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config.clone()).expect("open b");

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let done_b = done_tx.clone();
        // Signals completion even when the producer panics: without this the
        // consumer would wait forever for a signal that never arrives, and the
        // test would hang (observed once) instead of failing.
        struct Done(std::sync::mpsc::Sender<()>);
        impl Drop for Done {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }

        let join_a = std::thread::spawn(move || {
            let _done = Done(done_tx);
            for i in 0..PER_PRODUCER {
                // 0..60 payload bytes: single-slot and chunked messages mix.
                let payload = format!("a-{i}-{}", "x".repeat((i * 7) % 60));
                producer_a.send(payload.as_bytes()).expect("send a");
            }
        });
        let join_b = std::thread::spawn(move || {
            let _done = Done(done_b);
            for i in 0..PER_PRODUCER {
                let payload = format!("b-{i}-{}", "y".repeat((i * 13) % 60));
                producer_b.send(payload.as_bytes()).expect("send b");
            }
        });

        // Drain until both producers finished and the ring is empty. The
        // reservations are disjoint by construction, so every payload must
        // appear exactly once regardless of interleaving. The deadline turns
        // a stuck producer into a failure with a diagnostic rather than a
        // hung test binary.
        let mut consumer =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config).expect("open consumer");
        let mut received = Vec::with_capacity(2 * PER_PRODUCER);
        let mut producers_done = 0;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        loop {
            match consumer.recv_with_seq() {
                Ok(message) => received.push(String::from_utf8(message.payload).expect("utf8")),
                Err(DslineError::Channel(ChannelError::BufferEmpty)) => {
                    while done_rx.try_recv().is_ok() {
                        producers_done += 1;
                    }
                    if producers_done == 2 {
                        break;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "producers did not finish within 60s: {producers_done}/2 done, \
                         ring depth {}, received {}",
                        consumer.len(),
                        received.len()
                    );
                }
                Err(err) => panic!("unexpected recv error: {err:?}"),
            }
        }

        join_a.join().expect("join a");
        join_b.join().expect("join b");
        assert_eq!(received.len(), 2 * PER_PRODUCER);
        let mut expected: Vec<String> = Vec::with_capacity(2 * PER_PRODUCER);
        for i in 0..PER_PRODUCER {
            expected.push(format!("a-{i}-{}", "x".repeat((i * 7) % 60)));
            expected.push(format!("b-{i}-{}", "y".repeat((i * 13) % 60)));
        }
        let mut got = received.clone();
        got.sort();
        expected.sort();
        assert_eq!(got, expected, "every message delivered exactly once");

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn mpsc_drop_oldest_two_producers_never_drop_the_same_message_twice() {
        let path = test_file_path("mpsc-drop-oldest");
        let config = SpscConfig {
            capacity: 4,
            slot_size: 64,
            backpressure: Backpressure::DropOldest,
            timeout: None,
            multi_producer: true,
        };
        const PER_PRODUCER: usize = 100;
        let mut producer_a =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone()).expect("create");
        let mut producer_b =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config.clone()).expect("open b");
        // Both producers race on a full ring: their tail-drop CAS attempts
        // must never drop the same message twice (over-advance the tail).
        let join_a = std::thread::spawn(move || {
            for i in 0..PER_PRODUCER {
                let payload = format!("a-{i}");
                producer_a.send(payload.as_bytes()).expect("send a");
            }
        });
        let join_b = std::thread::spawn(move || {
            for i in 0..PER_PRODUCER {
                let payload = format!("b-{i}");
                producer_b.send(payload.as_bytes()).expect("send b");
            }
        });
        join_a.join().expect("join a");
        join_b.join().expect("join b");

        let mut consumer =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config).expect("open consumer");
        let mut survivors = Vec::new();
        while let Ok(message) = consumer.recv() {
            survivors.push(String::from_utf8(message).expect("utf8"));
        }
        let mut unique = survivors.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(survivors.len(), unique.len(), "no duplicate deliveries");
        assert!(!survivors.is_empty(), "the last messages must survive");
        assert!(survivors.len() <= 2 * PER_PRODUCER);
        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn mpsc_crashed_writer_leftover_counts_in_depth_and_surfaces_as_empty() {
        // MPSC zombie semantics: the reservation protocol already advanced
        // the head past the crashed producer's range, so the depth counts
        // the leftover with bias 0 — the send guard can never over-commit.
        let path = test_file_path("mpsc-crash");
        let config = mpsc_config(4);
        let mut channel =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone()).expect("create");
        channel.send(b"one").expect("send");
        // Crash mid-write: slot 1 holds a WRITING header (seq 1) and the
        // head cursor was reserved to 2 before the producer died.
        channel
            .region
            .write_header(1, SlotState::Writing, 1, 10)
            .expect("zombie header");
        channel.region.store_head(2).expect("head past zombie");

        let mut recovered =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config).expect("open");
        // Committed "one" + the reservation both count; no bias is needed.
        assert_eq!(recovered.len(), 2);
        // The producer writes after the reservation, never into the zombie.
        recovered.send(b"two").expect("send past zombie");
        assert_eq!(recovered.region.load_head().expect("head"), 3);

        assert_eq!(recovered.recv_with_seq().expect("recv one").payload, b"one");
        // The zombie is surfaced as an empty ring, not an infinite poll.
        assert!(matches!(
            recovered.recv(),
            Err(DslineError::Channel(ChannelError::BufferEmpty))
        ));

        fs::remove_file(path).expect("cleanup");
    }

    #[cfg(windows)]
    #[test]
    fn mpsc_section_backend_two_handles_exchange_in_order() {
        // The paging-file section backend provides the CAS reservation too:
        // two producer handles interleave sends through one section and the
        // consumer reads them back in reservation order.
        let path = test_file_path("mpsc-section");
        let config = mpsc_config(8);
        let mut a =
            PersistentShmChannel::<SectionShmSlotStorage>::create_or_open(&path, config.clone())
                .expect("producer a");
        let mut b =
            PersistentShmChannel::<SectionShmSlotStorage>::create_or_open(&path, config.clone())
                .expect("producer b");
        let mut consumer =
            PersistentShmChannel::<SectionShmSlotStorage>::create_or_open(&path, config.clone())
                .expect("consumer");

        a.send(b"alpha").expect("send alpha");
        b.send(b"beta").expect("send beta");
        let chunked = vec![b'c'; 90]; // 5-slot message
        a.send(&chunked).expect("send chunked");
        b.send(b"gamma").expect("send gamma");

        assert_eq!(consumer.recv_with_seq().expect("alpha").payload, b"alpha");
        assert_eq!(consumer.recv_with_seq().expect("beta").payload, b"beta");
        assert_eq!(consumer.recv_with_seq().expect("chunked").payload, chunked);
        assert_eq!(consumer.recv_with_seq().expect("gamma").payload, b"gamma");
        assert!(consumer.is_empty());
    }

    // ── Zero-copy alloc/publish (roadmap §4.2) ──

    fn zero_copy_config(capacity: usize, slot_size: usize) -> SpscConfig {
        persistent_config(capacity, slot_size)
    }

    #[test]
    fn zero_copy_alloc_publish_recv_view_roundtrip() {
        let path = test_file_path("zero-copy-roundtrip");
        let mut ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, zero_copy_config(4, 64))
                .expect("create");

        let payload: Vec<u8> = (0..32u8).collect();
        let mut lease = ch.alloc(payload.len()).expect("alloc");
        assert_eq!(lease.seq(), 0);
        assert_eq!(lease.size(), 32);
        lease.write(0, &payload).expect("write payload");
        let seq = ch.publish(&mut lease).expect("publish");
        assert_eq!(seq, 0);
        assert!(lease.is_sealed());

        let mut view = ch.recv_view().expect("recv_view");
        assert_eq!(view.seq(), 0);
        assert_eq!(view.size(), 32);
        assert_eq!(view.read_all().expect("read_all"), payload);
        // Partial reads see the same bytes without pulling the rest.
        assert_eq!(view.read(0, 4).expect("partial"), &payload[..4]);
        assert_eq!(view.read(28, 4).expect("partial tail"), &payload[28..]);

        view.release().expect("release");
        assert!(view.is_released());
        assert_eq!(ch.pinned_slots().expect("pinned"), 0);
        assert_eq!(ch.len(), 0);
    }

    #[test]
    fn zero_copy_seal_rejects_writes_and_double_publish() {
        let path = test_file_path("zero-copy-seal");
        let mut ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, zero_copy_config(2, 64))
                .expect("create");

        let mut lease = ch.alloc(8).expect("alloc");
        lease.write(0, b"abcdefgh").expect("write");

        ch.publish(&mut lease).expect("publish");
        assert_eq!(
            lease.write(0, b"X").expect_err("write after publish"),
            DslineError::Channel(ChannelError::InvalidConfig(
                "buffer is sealed: publish() already committed it"
            ))
        );
        assert_eq!(
            ch.publish(&mut lease).expect_err("double publish"),
            DslineError::Channel(ChannelError::InvalidConfig("buffer is already published"))
        );

        // Bounds are enforced before publish too.
        let lease2 = ch.alloc(4).expect("alloc 2");
        assert!(lease2.write(3, b"ab").is_err());
        assert!(lease2.write(0, b"abcde").is_err());
        ch.abort(lease2).expect("abort scratch");
    }

    #[test]
    fn zero_copy_abort_unwinds_and_reuses_the_slot() {
        let path = test_file_path("zero-copy-abort");
        let mut ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, zero_copy_config(2, 64))
                .expect("create");

        let lease = ch.alloc(8).expect("alloc");
        let aborted_seq = lease.seq();
        ch.abort(lease).expect("abort");

        // The slot is reusable and the sequence rewound: the next send
        // reuses both.
        ch.send(b"hello").expect("send after abort");
        let message = ch.recv_with_seq().expect("recv");
        assert_eq!(message.seq, aborted_seq);
        assert_eq!(message.payload, b"hello");

        // Only the newest reservation can be unwound.
        let first = ch.alloc(4).expect("alloc first");
        let _second = ch.alloc(4).expect("alloc second");
        assert_eq!(
            ch.abort(first).expect_err("abort non-newest"),
            DslineError::Channel(ChannelError::InvalidConfig(
                "only the newest unpublished buffer can be aborted"
            ))
        );
    }

    #[test]
    fn zero_copy_view_pins_the_ring_until_release() {
        let path = test_file_path("zero-copy-pin-guard");
        let mut ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, zero_copy_config(4, 64))
                .expect("create");

        ch.send(b"m0").expect("send 0");
        ch.send(b"m1").expect("send 1");
        ch.send(b"m2").expect("send 2");
        let mut lease = ch.alloc(2).expect("alloc");
        lease.write(0, b"m3").expect("write");
        ch.publish(&mut lease).expect("publish");

        // Pin slot 0 (cursor 0); cursors 1..3 are consumed normally.
        let mut view = ch.recv_view().expect("recv_view");
        assert_eq!(view.read_all().expect("read"), b"m0");
        assert_eq!(ch.pinned_slots().expect("pinned"), 1);
        ch.recv().expect("consume m1");
        ch.recv().expect("consume m2");

        // The ring is logically empty but the pin floor holds cursor 0:
        // a send would claim cursor 4 = the same physical slot 0.
        assert_eq!(
            ch.send(b"m4").expect_err("send blocked by pin"),
            DslineError::Channel(ChannelError::BufferFull)
        );
        // The pinned bytes are untouched by the failed attempts.
        assert_eq!(view.read_all().expect("read still"), b"m0");

        // Releasing raises the floor back to "none" and un-shrinks the
        // guard; the ring wraps onto the released slot.
        view.release().expect("release");
        ch.send(b"m4").expect("send after release");
        assert_eq!(ch.recv().expect("recv m3"), b"m3");
        assert_eq!(ch.recv().expect("recv m4"), b"m4");
    }

    #[test]
    fn zero_copy_two_views_out_of_order_release_keeps_floor() {
        let path = test_file_path("zero-copy-two-views");
        let mut ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, zero_copy_config(4, 64))
                .expect("create");

        for i in 0..3 {
            ch.send(format!("m{i}").as_bytes()).expect("send");
        }
        let mut older = ch.recv_view().expect("view older");
        let mut newer = ch.recv_view().expect("view newer");

        // Releasing the newer view must not unblock the ring: the older
        // pin still owns the floor.
        newer.release().expect("release newer");
        for i in 3..4 {
            ch.send(format!("m{i}").as_bytes()).expect("fill");
        }
        assert_eq!(
            ch.send(b"m4").expect_err("still pinned"),
            DslineError::Channel(ChannelError::BufferFull)
        );

        older.release().expect("release older");
        ch.send(b"m4").expect("send after both released");
    }

    #[test]
    fn zero_copy_recv_view_rejects_chunked_and_drop_oldest() {
        let path = test_file_path("zero-copy-chunked");
        let mut ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, zero_copy_config(8, 64))
                .expect("create");
        // 100 payload bytes at 34 payload bytes per chunk slot = a
        // 3-slot chunked message.
        ch.send(&[7u8; 100]).expect("send chunked");
        assert!(matches!(
            ch.recv_view(),
            Err(DslineError::Channel(ChannelError::InvalidConfig(_)))
        ));
        // The chunked message is untouched: the copy path still gets it.
        assert_eq!(ch.recv().expect("copy path"), vec![7u8; 100]);
    }

    #[test]
    fn zero_copy_alloc_rejected_on_mpsc_and_drop_oldest() {
        let path = test_file_path("zero-copy-rejects");
        let mut mpsc_config = zero_copy_config(4, 64);
        mpsc_config.multi_producer = true;
        let mut ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, mpsc_config).expect("create");
        assert!(matches!(
            ch.alloc(8),
            Err(DslineError::Channel(ChannelError::InvalidConfig(_)))
        ));

        let path2 = test_file_path("zero-copy-drop-oldest");
        let mut drop_config = zero_copy_config(4, 64);
        drop_config.backpressure = Backpressure::DropOldest;
        let mut ch2 =
            PersistentShmChannel::<MmapSlotStorage>::create(&path2, drop_config).expect("create");
        assert!(matches!(
            ch2.alloc(8),
            Err(DslineError::Channel(ChannelError::InvalidConfig(_)))
        ));
        ch2.send(b"m").expect("send");
        assert!(matches!(
            ch2.recv_view(),
            Err(DslineError::Channel(ChannelError::InvalidConfig(_)))
        ));
    }

    #[test]
    fn zero_copy_recv_view_detects_corrupted_payload() {
        let path = test_file_path("zero-copy-corrupt");
        let mut ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, zero_copy_config(2, 64))
                .expect("create");

        let mut lease = ch.alloc(8).expect("alloc");
        lease.write(0, b"payload!").expect("write");
        ch.publish(&mut lease).expect("publish");

        // Corrupt the payload in place (simulating bit rot between publish
        // and receive): the view's in-place checksum verify must catch it.
        let corrupt_at = region_slot_offset(0, ch.region.slot_size()) + ZERO_COPY_PAYLOAD_OFFSET;
        ch.region
            .storage
            .write_at(corrupt_at, b"X")
            .expect("corrupt");
        assert!(matches!(
            ch.recv_view(),
            Err(DslineError::Channel(ChannelError::CorruptedMessage))
        ));
    }

    #[test]
    #[cfg(any(windows, target_os = "linux"))]
    fn zero_copy_open_reclaims_dead_consumer_pins() {
        let path = test_file_path("zero-copy-dead-pins");
        let (dead_pid, dead_start) = spawn_dead_identity();

        // Simulate a consumer that pinned a view and died: a PINNED header
        // with its (dead) identity, and the floor pointing at it.
        {
            let mut ch =
                PersistentShmChannel::<MmapSlotStorage>::create(&path, zero_copy_config(4, 64))
                    .expect("create");
            ch.send(b"m0").expect("send");
            let stamp = WriterIdentity {
                pid: dead_pid,
                start_time: dead_start,
                epoch: 7,
                timestamp_ns: 0,
                reserved_slots: 1,
            };
            // frame_len = FRAME_HEADER_LEN + 2 payload bytes.
            ch.region
                .write_pinned_header(0, 0, FRAME_HEADER_LEN + 2, stamp)
                .expect("write pinned header");
            ch.region.store_floor(0).expect("store floor");
            // A real pin is consumed: the tail cursor advanced past it.
            ch.region.fetch_max_tail(1).expect("advance tail");
        }

        // Reopening rebuilds the state: the dead pin is reclaimed (the
        // message was lost with its consumer), the floor returns to the
        // "no pins" sentinel, and the ring is fully writable again.
        let mut ch = PersistentShmChannel::<MmapSlotStorage>::open(&path, zero_copy_config(4, 64))
            .expect("reopen");
        assert_eq!(ch.reclaimed_slots, 1);
        assert_eq!(ch.region.load_floor().expect("floor"), PIN_FLOOR_NONE);
        assert!(ch.recv().is_err(), "pinned message died with its consumer");
        for i in 0..4 {
            ch.send(format!("n{i}").as_bytes())
                .expect("ring un-shrunk after reclaim");
        }
    }

    #[test]
    fn zero_copy_second_handle_rebuilds_floor_for_live_pins() {
        let path = test_file_path("zero-copy-live-pins");
        let mut owner =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, zero_copy_config(4, 64))
                .expect("create");
        owner.send(b"m0").expect("send");
        let mut view = owner.recv_view().expect("pin cursor 0");

        // A second handle opens while the pin is live: the identity
        // classifies as Alive, so the pin survives and the floor rebuilds
        // to its cursor — the opener's send guard respects the pin.
        let mut other =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, zero_copy_config(4, 64))
                .expect("open second handle");
        assert_eq!(other.reclaimed_slots, 0);
        other.send(b"m1").expect("send 1");
        other.send(b"m2").expect("send 2");
        other.send(b"m3").expect("send 3");
        assert_eq!(
            other.send(b"m4").expect_err("pin blocks the other handle"),
            DslineError::Channel(ChannelError::BufferFull)
        );

        view.release().expect("release");
        other.send(b"m4").expect("send after release");
    }

    #[test]
    #[cfg(windows)]
    fn zero_copy_roundtrip_section_backend() {
        let path = test_file_path("zero-copy-section");
        let mut ch =
            PersistentShmChannel::<SectionShmSlotStorage>::create(&path, zero_copy_config(4, 64))
                .expect("create");

        let payload: Vec<u8> = (b'a'..b'z').collect();
        let mut lease = ch.alloc(payload.len()).expect("alloc");
        lease.write(0, &payload).expect("write");
        ch.publish(&mut lease).expect("publish");

        let mut view = ch.recv_view().expect("recv_view");
        assert_eq!(view.read_all().expect("read"), payload);
        view.release().expect("release");
        assert_eq!(ch.pinned_slots().expect("pinned"), 0);
    }

    #[test]
    fn zero_copy_floor_sentinel_tracks_no_pins() {
        // With zero-copy never used, the floor stays at the sentinel and
        // the guard reduces to the plain cursor depth (regression guard
        // for the PIN_FLOOR_NONE initialization).
        let path = test_file_path("zero-copy-sentinel");
        let mut ch =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, zero_copy_config(8, 64))
                .expect("create");
        assert_eq!(ch.region.load_floor().expect("floor"), PIN_FLOOR_NONE);
        ch.send(b"a").expect("send a");
        ch.recv().expect("recv a");
        ch.send(b"b").expect("send b");
        ch.send(b"c").expect("send c");
        ch.send(b"d").expect("send d");
        ch.send(b"e").expect("send e");
        ch.send(b"f").expect("send f");
    }

    /// Temporary probe: where does the copy path's per-byte cost come from?
    /// Run with `cargo test -p dsline-shm --lib timing_probe -- --nocapture`.
    #[test]
    #[ignore]
    fn timing_probe() {
        use dsline_core::checksum::checksum32;
        use std::time::Instant;

        const N: usize = 5000;
        let report = |label: &str, total: std::time::Duration| {
            println!(
                "{label:<28} {:>8.2} µs/call",
                total.as_secs_f64() * 1e6 / N as f64
            );
        };

        for size in [64usize, 4096] {
            println!("--- payload {size} B ---");
            let payload = vec![b'z'; size];

            // 1. checksum alone
            let t = Instant::now();
            let mut sink = 0u32;
            for _ in 0..N {
                sink = sink.wrapping_add(checksum32(&payload));
            }
            report("checksum32", t.elapsed());
            assert_ne!(sink, 0);

            // 2. frame encode alone
            let t = Instant::now();
            for _ in 0..N {
                let frame = dsline_core::encode_frame(
                    dsline_core::FrameKind::Bytes,
                    0,
                    1,
                    0,
                    0,
                    &[],
                    &payload,
                )
                .expect("encode");
                sink = sink.wrapping_add(frame.len() as u32);
            }
            report("encode_frame", t.elapsed());
            assert_ne!(sink, 0);

            // 3. region write alone (frame already built)
            let path = test_file_path(&format!("timing-write-{size}"));
            let frame =
                dsline_core::encode_frame(dsline_core::FrameKind::Bytes, 0, 1, 0, 0, &[], &payload)
                    .expect("encode");
            let region = PersistentSlotRegion::<MmapSlotStorage>::create(&path, N, size + 98)
                .expect("region");
            let t = Instant::now();
            for index in 0..N {
                region
                    .write_committed(index, 1, &frame, None)
                    .expect("write committed");
            }
            report("region write_committed", t.elapsed());
            drop(region);
            fs::remove_file(&path).expect("cleanup");

            // 4. region read alone
            let path = test_file_path(&format!("timing-read-{size}"));
            let region = PersistentSlotRegion::<MmapSlotStorage>::create(&path, N, size + 98)
                .expect("region");
            for index in 0..N {
                region
                    .write_committed(index, 1, &frame, None)
                    .expect("write committed");
            }
            let t = Instant::now();
            for index in 0..N {
                let snapshot = region.read_committed(index).expect("read");
                sink = sink.wrapping_add(snapshot.data.len() as u32);
                region.release_pinned(index).expect("release");
            }
            report("region read_committed", t.elapsed());
            drop(region);
            fs::remove_file(&path).expect("cleanup");

            // 5. full channel send + try_recv
            let path = test_file_path(&format!("timing-channel-{size}"));
            let config = SpscConfig {
                capacity: N,
                slot_size: size + 64,
                backpressure: Backpressure::Raise,
                timeout: None,
                multi_producer: false,
            };
            let mut producer =
                PersistentShmChannel::<MmapSlotStorage>::create(&path, config.clone())
                    .expect("producer");
            let mut consumer =
                PersistentShmChannel::<MmapSlotStorage>::open(&path, config).expect("consumer");
            let t = Instant::now();
            for _ in 0..N {
                producer.send(&payload).expect("send");
            }
            report("channel send", t.elapsed());
            let t = Instant::now();
            for _ in 0..N {
                sink = sink.wrapping_add(
                    consumer
                        .try_recv()
                        .expect("try_recv")
                        .expect("readable")
                        .len() as u32,
                );
            }
            report("channel try_recv", t.elapsed());
            assert_ne!(sink, 0);
            drop(producer);
            drop(consumer);
            fs::remove_file(&path).expect("cleanup");
        }
    }
}
