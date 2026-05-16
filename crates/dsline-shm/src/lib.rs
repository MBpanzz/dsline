//! Shared-memory backend.
//!
//! The 0.0.1 target is a Linux-first fixed-slot SPSC bytes backend. The public
//! API is intentionally conservative until ADR-001 is accepted.

use dsline_core::{
    checksum::checksum32, decode_frame, encode_frame, Backpressure, ChannelError, FrameKind,
    Result, SpscConfig, FRAME_HEADER_LEN,
};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread;
use std::time::Instant;

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

#[derive(Debug)]
pub struct FileSlotStorage {
    capacity: usize,
    slot_size: usize,
    path: PathBuf,
    file: Mutex<File>,
}

impl FileSlotStorage {
    pub fn create(path: impl AsRef<Path>, capacity: usize, slot_size: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(ChannelError::InvalidConfig("capacity must be greater than zero").into());
        }
        if slot_size == 0 {
            return Err(ChannelError::InvalidConfig("slot_size must be greater than zero").into());
        }
        let total_len = capacity
            .checked_mul(slot_size)
            .ok_or(ChannelError::InvalidConfig("storage size overflow"))?;
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

    pub fn open(path: impl AsRef<Path>, capacity: usize, slot_size: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(ChannelError::InvalidConfig("capacity must be greater than zero").into());
        }
        if slot_size == 0 {
            return Err(ChannelError::InvalidConfig("slot_size must be greater than zero").into());
        }
        let expected_len = capacity
            .checked_mul(slot_size)
            .ok_or(ChannelError::InvalidConfig("storage size overflow"))?;
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
        if actual_len < expected_len as u64 {
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

        let offset = self.offset(index)?;
        let mut file = self
            .file
            .lock()
            .map_err(|_| ChannelError::StorageIo("file storage lock poisoned"))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|_| ChannelError::StorageIo("seek file storage"))?;
        file.write_all(bytes)
            .map_err(|_| ChannelError::StorageIo("write file storage"))?;
        file.flush()
            .map_err(|_| ChannelError::StorageIo("flush file storage"))?;
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
        let mut data = vec![0; len];
        let mut file = self
            .file
            .lock()
            .map_err(|_| ChannelError::StorageIo("file storage lock poisoned"))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|_| ChannelError::StorageIo("seek file storage"))?;
        file.read_exact(&mut data)
            .map_err(|_| ChannelError::StorageIo("read file storage"))?;
        Ok(data)
    }

    fn clear_slot(&mut self, index: usize) -> Result<()> {
        let offset = self.offset(index)?;
        let zeros = vec![0; self.slot_size];
        let mut file = self
            .file
            .lock()
            .map_err(|_| ChannelError::StorageIo("file storage lock poisoned"))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|_| ChannelError::StorageIo("seek file storage"))?;
        file.write_all(&zeros)
            .map_err(|_| ChannelError::StorageIo("clear file storage"))?;
        file.flush()
            .map_err(|_| ChannelError::StorageIo("flush file storage"))?;
        Ok(())
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

    pub fn release_pinned(&mut self, index: usize) -> Result<()> {
        if self.slot(index)?.state != SlotState::Pinned {
            return Err(ChannelError::InvalidConfig("slot is not pinned").into());
        }

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

// ── Persistent slot region (file-backed, cross-process ready) ──

/// Size of the per-slot header stored in persistent storage.
pub const PERSISTENT_SLOT_HEADER_LEN: usize = 13;

/// A slot region that stores metadata (state, seq, length) in the storage
/// layer alongside frame data, enabling cross-process access.
///
/// Slot layout in storage:
/// ```text
/// [state: u8] [seq: u64 LE] [payload_len: u32 LE] [frame bytes…]
/// ```
#[derive(Debug)]
pub struct PersistentSlotRegion {
    storage: FileSlotStorage,
    /// Usable frame space after subtracting the header.
    frame_capacity: usize,
}

impl PersistentSlotRegion {
    /// Create a new file-backed persistent region.
    ///
    /// `slot_size` is the total per-slot size (header + max frame).
    pub fn create(path: impl AsRef<Path>, capacity: usize, slot_size: usize) -> Result<Self> {
        if slot_size <= PERSISTENT_SLOT_HEADER_LEN {
            return Err(ChannelError::InvalidConfig(
                "slot_size must be greater than persistent header size",
            )
            .into());
        }
        let storage = FileSlotStorage::create(path, capacity, slot_size)?;
        Ok(Self {
            frame_capacity: slot_size - PERSISTENT_SLOT_HEADER_LEN,
            storage,
        })
    }

    /// Open an existing persistent region.
    pub fn open(path: impl AsRef<Path>, capacity: usize, slot_size: usize) -> Result<Self> {
        if slot_size <= PERSISTENT_SLOT_HEADER_LEN {
            return Err(ChannelError::InvalidConfig(
                "slot_size must be greater than persistent header size",
            )
            .into());
        }
        let storage = FileSlotStorage::open(path, capacity, slot_size)?;
        Ok(Self {
            frame_capacity: slot_size - PERSISTENT_SLOT_HEADER_LEN,
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
        let raw = self.storage.read_slot(index, PERSISTENT_SLOT_HEADER_LEN)?;
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
        let offset = self.slot_offset(index);
        let mut file = self
            .storage
            .file
            .lock()
            .map_err(|_| ChannelError::StorageIo("persistent storage lock poisoned"))?;
        file.seek(SeekFrom::Start(offset as u64))
            .map_err(|_| ChannelError::StorageIo("seek header write"))?;
        file.write_all(&header)
            .map_err(|_| ChannelError::StorageIo("write header"))?;
        file.flush()
            .map_err(|_| ChannelError::StorageIo("flush header"))?;
        Ok(())
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
        let offset = self.slot_offset(index) + PERSISTENT_SLOT_HEADER_LEN;
        let mut file = self
            .storage
            .file
            .lock()
            .map_err(|_| ChannelError::StorageIo("persistent storage lock poisoned"))?;
        file.seek(SeekFrom::Start(offset as u64))
            .map_err(|_| ChannelError::StorageIo("seek persistent payload"))?;
        file.write_all(frame)
            .map_err(|_| ChannelError::StorageIo("write persistent payload"))?;
        file.flush()
            .map_err(|_| ChannelError::StorageIo("flush persistent payload"))?;
        Ok(())
    }

    fn read_payload(&self, index: usize, len: usize) -> Result<Vec<u8>> {
        if len > self.frame_capacity {
            return Err(ChannelError::MessageTooLarge {
                len,
                slot_size: self.frame_capacity,
            }
            .into());
        }
        let offset = self.slot_offset(index) + PERSISTENT_SLOT_HEADER_LEN;
        let mut data = vec![0u8; len];
        let mut file = self
            .storage
            .file
            .lock()
            .map_err(|_| ChannelError::StorageIo("persistent storage lock poisoned"))?;
        file.seek(SeekFrom::Start(offset as u64))
            .map_err(|_| ChannelError::StorageIo("seek persistent payload"))?;
        file.read_exact(&mut data)
            .map_err(|_| ChannelError::StorageIo("read persistent payload"))?;
        Ok(data)
    }

    fn clear_payload(&self, index: usize) -> Result<()> {
        let offset = self.slot_offset(index) + PERSISTENT_SLOT_HEADER_LEN;
        let zeros = vec![0u8; self.frame_capacity];
        let mut file = self
            .storage
            .file
            .lock()
            .map_err(|_| ChannelError::StorageIo("persistent storage lock poisoned"))?;
        file.seek(SeekFrom::Start(offset as u64))
            .map_err(|_| ChannelError::StorageIo("seek persistent clear"))?;
        file.write_all(&zeros)
            .map_err(|_| ChannelError::StorageIo("clear persistent payload"))?;
        file.flush()
            .map_err(|_| ChannelError::StorageIo("flush persistent clear"))?;
        Ok(())
    }

    fn slot_offset(&self, index: usize) -> usize {
        index * self.storage.slot_size()
    }

    // ── public state-transition API ──

    /// Query the current state of a slot from storage.
    pub fn state(&self, index: usize) -> Result<SlotState> {
        Ok(self.read_header(index)?.0)
    }

    /// Write a committed frame to a free slot.
    ///
    /// Protocol: write WRITING header → write payload → write COMMITTED header.
    pub fn write_committed(&self, index: usize, seq: u64, frame: &[u8]) -> Result<()> {
        let (state, _, _) = self.read_header(index)?;
        if state != SlotState::Free {
            return Err(ChannelError::BufferFull.into());
        }

        // Phase 1: mark WRITING
        self.write_header(index, SlotState::Writing, seq, frame.len())?;

        // Phase 2: write payload
        self.write_payload(index, frame)?;

        // Phase 3: commit
        self.write_header(index, SlotState::Committed, seq, frame.len())?;

        Ok(())
    }

    /// Read a committed frame, transitioning the slot to PINNED.
    pub fn read_committed(&self, index: usize) -> Result<SlotSnapshot> {
        let (state, seq, payload_len) = self.read_header(index)?;
        if state != SlotState::Committed {
            return Err(ChannelError::BufferEmpty.into());
        }

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

    /// Release a pinned slot back to FREE.
    pub fn release_pinned(&self, index: usize) -> Result<()> {
        let (state, _, _) = self.read_header(index)?;
        if state != SlotState::Pinned {
            return Err(ChannelError::InvalidConfig("slot is not pinned").into());
        }
        self.clear_payload(index)?;
        self.write_header(index, SlotState::Free, 0, 0)?;
        Ok(())
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

/// A channel backed by a persistent (file-based) slot region.
///
/// This channel can be opened by multiple processes that share the same
/// backing file. One process acts as producer, another as consumer.
#[derive(Debug)]
pub struct PersistentShmChannel {
    region: PersistentSlotRegion,
    config: SpscConfig,
    head: usize,
    tail: usize,
    len: usize,
    next_seq: u64,
    expected_recv_seq: u64,
    last_received_seq: Option<u64>,
    closed: bool,
}

impl PersistentShmChannel {
    /// Create a new persistent channel, initialising the file.
    pub fn create(path: impl AsRef<Path>, config: SpscConfig) -> Result<Self> {
        config.validate()?;
        let region_slot_size = config
            .slot_size
            .checked_add(FRAME_HEADER_LEN)
            .and_then(|v| v.checked_add(PERSISTENT_SLOT_HEADER_LEN))
            .ok_or(ChannelError::InvalidConfig("slot_size is too large"))?;

        let region = PersistentSlotRegion::create(path, config.capacity, region_slot_size)?;

        // Initialise all slots as FREE.
        for i in 0..config.capacity {
            region.write_header(i, SlotState::Free, 0, 0)?;
        }

        Ok(Self {
            region,
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

    /// Open an existing persistent channel.
    ///
    /// The caller is responsible for knowing whether it is the producer or
    /// consumer. This method reads the current state from the file to
    /// recover `head` and `tail` positions.
    pub fn open(path: impl AsRef<Path>, config: SpscConfig) -> Result<Self> {
        config.validate()?;
        let region_slot_size = config
            .slot_size
            .checked_add(FRAME_HEADER_LEN)
            .and_then(|v| v.checked_add(PERSISTENT_SLOT_HEADER_LEN))
            .ok_or(ChannelError::InvalidConfig("slot_size is too large"))?;

        let region = PersistentSlotRegion::open(path, config.capacity, region_slot_size)?;

        // Scan the ring to recover state.
        let mut len = 0usize;
        let mut next_seq = 0u64;
        let mut head = 0usize;

        for i in 0..config.capacity {
            if let Ok((
                state @ (SlotState::Committed | SlotState::Pinned | SlotState::Writing),
                seq,
                _,
            )) = region.read_header(i)
            {
                len += 1;
                head = (i + 1) % config.capacity;
                next_seq = next_seq.max(seq.wrapping_add(1));
                let _ = state; // used only in pattern
            }
        }

        // Tail is the first non-FREE slot.
        let tail = (0..config.capacity)
            .find(|&i| {
                matches!(
                    region.read_header(i),
                    Ok((SlotState::Committed, _, _))
                        | Ok((SlotState::Pinned, _, _))
                        | Ok((SlotState::Writing, _, _))
                )
            })
            .unwrap_or(0);

        // expected_recv_seq is the seq of the first committed slot.
        let expected_recv = if len > 0 {
            region
                .read_header(tail)
                .map(|(_, s, _)| s)
                .unwrap_or(next_seq)
        } else {
            next_seq
        };

        Ok(Self {
            region,
            config,
            head,
            tail,
            len,
            next_seq,
            expected_recv_seq: expected_recv,
            last_received_seq: None,
            closed: false,
        })
    }

    pub fn send(&mut self, bytes: &[u8]) -> Result<()> {
        if self.closed {
            return Err(ChannelError::Closed.into());
        }
        if bytes.len() > self.config.slot_size {
            return Err(ChannelError::MessageTooLarge {
                len: bytes.len(),
                slot_size: self.config.slot_size,
            }
            .into());
        }

        if self.config.backpressure == Backpressure::Raise && self.len == self.config.capacity {
            return Err(ChannelError::BufferFull.into());
        }
        if self.config.backpressure == Backpressure::Block {
            self.wait_for_space()?;
        }

        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        let frame = encode_frame(FrameKind::Bytes, 0, seq, 0, 0, &[], bytes)?;
        self.region.write_committed(self.head, seq, &frame)?;
        self.head = (self.head + 1) % self.config.capacity;
        self.len += 1;
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

        // Poll until the slot at tail is COMMITTED.
        loop {
            let (state, _, _) = self.region.read_header(self.tail)?;
            if state == SlotState::Committed {
                break;
            }
            if self.closed {
                return Err(ChannelError::Closed.into());
            }
            thread::yield_now();
        }

        let snapshot = self.region.read_committed(self.tail)?;
        let decoded = decode_frame(&snapshot.data);
        self.region.release_pinned(self.tail)?;
        self.tail = (self.tail + 1) % self.config.capacity;
        self.len -= 1;

        if snapshot.seq != self.expected_recv_seq {
            return Err(ChannelError::SequenceMismatch {
                expected: self.expected_recv_seq,
                actual: snapshot.seq,
            }
            .into());
        }
        self.expected_recv_seq = self.expected_recv_seq.wrapping_add(1);

        let frame = decoded?;
        if frame.header.kind != FrameKind::Bytes
            || checksum32(&frame.payload) != frame.header.checksum
        {
            return Err(ChannelError::CorruptedMessage.into());
        }

        if snapshot.seq != frame.header.seq {
            return Err(ChannelError::SequenceMismatch {
                expected: snapshot.seq,
                actual: frame.header.seq,
            }
            .into());
        }

        let seq = frame.header.seq;
        self.last_received_seq = Some(seq);
        Ok(ReceivedMessage {
            seq,
            payload: frame.payload,
        })
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

    fn wait_for_space(&self) -> Result<()> {
        if self.len < self.config.capacity {
            return Ok(());
        }
        let Some(timeout) = self.config.timeout else {
            return Err(ChannelError::BufferFull.into());
        };
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.len < self.config.capacity {
                return Ok(());
            }
            thread::yield_now();
        }
        Err(ChannelError::BufferFull.into())
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
        if bytes.len() > self.config.slot_size {
            return Err(ChannelError::MessageTooLarge {
                len: bytes.len(),
                slot_size: self.config.slot_size,
            }
            .into());
        }

        match self.config.backpressure {
            Backpressure::Raise if self.len == self.config.capacity => {
                return Err(ChannelError::BufferFull.into());
            }
            Backpressure::Block => self.wait_for_space()?,
            Backpressure::Raise => {}
        }

        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        let frame = encode_frame(FrameKind::Bytes, 0, seq, 0, 0, &[], bytes)?;
        self.region.write_committed(self.head, seq, &frame)?;
        self.head = (self.head + 1) % self.config.capacity;
        self.len += 1;
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
        let decoded = decode_frame(&snapshot.data);
        self.region.release_pinned(self.tail)?;
        self.tail = (self.tail + 1) % self.config.capacity;
        self.len -= 1;

        if snapshot.seq != self.expected_recv_seq {
            return Err(ChannelError::SequenceMismatch {
                expected: self.expected_recv_seq,
                actual: snapshot.seq,
            }
            .into());
        }
        self.expected_recv_seq = self.expected_recv_seq.wrapping_add(1);

        let frame = decoded?;
        if frame.header.kind != FrameKind::Bytes
            || checksum32(&frame.payload) != frame.header.checksum
        {
            return Err(ChannelError::CorruptedMessage.into());
        }

        if snapshot.seq != frame.header.seq {
            return Err(ChannelError::SequenceMismatch {
                expected: snapshot.seq,
                actual: frame.header.seq,
            }
            .into());
        }

        let seq = frame.header.seq;
        self.last_received_seq = Some(seq);

        Ok(ReceivedMessage {
            seq,
            payload: frame.payload,
        })
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

    fn wait_for_space(&self) -> Result<()> {
        if self.len < self.config.capacity {
            return Ok(());
        }
        let Some(timeout) = self.config.timeout else {
            return Err(ChannelError::BufferFull.into());
        };
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.len < self.config.capacity {
                return Ok(());
            }
            thread::yield_now();
        }
        Err(ChannelError::BufferFull.into())
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
        FileSlotStorage, FixedSlotRegion, FixedSlotStorage, MemorySlotStorage, ShmSpscChannel,
        SlotState,
    };
    use dsline_core::error::{ChannelError, DslineError};
    use dsline_core::{
        decode_frame, encode_frame, Backpressure, FrameKind, SpscConfig, FRAME_HEADER_LEN,
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
        let mut storage = FileSlotStorage::create(&path, 2, 16).expect("storage");

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
        let storage = FileSlotStorage::create(&path, 2, 16 + FRAME_HEADER_LEN).expect("storage");
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
            FileSlotStorage::open(&path, 2, 16).expect_err("too small"),
            DslineError::Channel(ChannelError::InvalidConfig(
                "file storage is smaller than expected"
            ))
        );

        fs::remove_file(path).expect("cleanup");
    }
}
