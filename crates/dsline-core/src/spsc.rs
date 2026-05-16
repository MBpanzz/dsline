use crate::checksum::checksum32;
use crate::error::{ChannelError, Result};
use crate::frame::{decode_frame, encode_frame, FrameKind};
use std::collections::VecDeque;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backpressure {
    Block,
    Raise,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpscConfig {
    pub capacity: usize,
    pub slot_size: usize,
    pub backpressure: Backpressure,
    pub timeout: Option<Duration>,
}

impl SpscConfig {
    pub fn validate(&self) -> Result<()> {
        if self.capacity == 0 {
            return Err(ChannelError::InvalidConfig("capacity must be greater than zero").into());
        }
        if self.slot_size == 0 {
            return Err(ChannelError::InvalidConfig("slot_size must be greater than zero").into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Slot {
    frame: Vec<u8>,
}

/// Fixed-slot SPSC bytes ring for the 0.0.1 prototype.
///
/// This in-memory implementation establishes the public state-machine behavior
/// before the storage is moved behind a shared-memory backend.
#[derive(Debug)]
pub struct SpscBytesRing {
    config: SpscConfig,
    queue: VecDeque<Slot>,
    next_seq: u64,
    closed: bool,
}

impl SpscBytesRing {
    pub fn new(config: SpscConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            queue: VecDeque::with_capacity(config.capacity),
            config,
            next_seq: 0,
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
            Backpressure::Raise if self.queue.len() == self.config.capacity => {
                return Err(ChannelError::BufferFull.into());
            }
            Backpressure::Block => self.wait_for_space()?,
            Backpressure::Raise => {}
        }

        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        let frame = encode_frame(FrameKind::Bytes, 0, seq, 0, 0, &[], bytes)?;
        self.queue.push_back(Slot { frame });
        Ok(())
    }

    pub fn recv(&mut self) -> Result<Vec<u8>> {
        if self.closed && self.queue.is_empty() {
            return Err(ChannelError::Closed.into());
        }

        let slot = match self.queue.pop_front() {
            Some(slot) => slot,
            None => return Err(ChannelError::BufferEmpty.into()),
        };
        let frame = decode_frame(&slot.frame)?;
        if frame.header.kind != FrameKind::Bytes
            || checksum32(&frame.payload) != frame.header.checksum
        {
            return Err(ChannelError::CorruptedMessage.into());
        }
        Ok(frame.payload)
    }

    pub fn close(&mut self) {
        self.closed = true;
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.config.capacity
    }

    fn wait_for_space(&self) -> Result<()> {
        if self.queue.len() < self.config.capacity {
            return Ok(());
        }
        let Some(timeout) = self.config.timeout else {
            return Err(ChannelError::BufferFull.into());
        };
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.queue.len() < self.config.capacity {
                return Ok(());
            }
            thread::yield_now();
        }
        Err(ChannelError::BufferFull.into())
    }

    #[cfg(test)]
    fn corrupt_next_checksum_for_test(&mut self) {
        if let Some(slot) = self.queue.front_mut() {
            let checksum_offset = 46;
            let checksum = u32::from_le_bytes([
                slot.frame[checksum_offset],
                slot.frame[checksum_offset + 1],
                slot.frame[checksum_offset + 2],
                slot.frame[checksum_offset + 3],
            ]);
            slot.frame[checksum_offset..checksum_offset + 4]
                .copy_from_slice(&checksum.wrapping_add(1).to_le_bytes());
        }
    }

    #[cfg(test)]
    fn next_seq_for_test(&self) -> u64 {
        self.next_seq
    }
}

#[cfg(test)]
mod tests {
    use super::{Backpressure, SpscBytesRing, SpscConfig};
    use crate::error::{ChannelError, DslineError};
    use std::time::Duration;

    fn ring(capacity: usize, slot_size: usize) -> SpscBytesRing {
        SpscBytesRing::new(SpscConfig {
            capacity,
            slot_size,
            backpressure: Backpressure::Raise,
            timeout: None,
        })
        .expect("valid config")
    }

    #[test]
    fn sends_and_receives_bytes_in_order() {
        let mut ring = ring(4, 16);
        ring.send(b"one").expect("send one");
        ring.send(b"two").expect("send two");

        assert_eq!(ring.recv().expect("recv one"), b"one");
        assert_eq!(ring.recv().expect("recv two"), b"two");
    }

    #[test]
    fn rejects_oversized_messages() {
        let mut ring = ring(4, 3);
        let err = ring.send(b"four").expect_err("message too large");

        assert_eq!(
            err,
            DslineError::Channel(ChannelError::MessageTooLarge {
                len: 4,
                slot_size: 3
            })
        );
    }

    #[test]
    fn raise_backpressure_reports_full() {
        let mut ring = ring(1, 8);
        ring.send(b"a").expect("first send");

        assert_eq!(
            ring.send(b"b").expect_err("full"),
            DslineError::Channel(ChannelError::BufferFull)
        );
    }

    #[test]
    fn block_backpressure_times_out_until_shared_storage_exists() {
        let mut ring = SpscBytesRing::new(SpscConfig {
            capacity: 1,
            slot_size: 8,
            backpressure: Backpressure::Block,
            timeout: Some(Duration::from_millis(1)),
        })
        .expect("valid config");
        ring.send(b"a").expect("first send");

        assert_eq!(
            ring.send(b"b").expect_err("timeout"),
            DslineError::Channel(ChannelError::BufferFull)
        );
    }

    #[test]
    fn detects_checksum_mismatch() {
        let mut ring = ring(1, 8);
        ring.send(b"a").expect("send");
        ring.corrupt_next_checksum_for_test();

        assert_eq!(
            ring.recv().expect_err("corrupted"),
            DslineError::Channel(ChannelError::CorruptedMessage)
        );
    }

    #[test]
    fn close_prevents_send_and_drains_existing_messages() {
        let mut ring = ring(2, 8);
        ring.send(b"a").expect("send");
        ring.close();

        assert_eq!(
            ring.send(b"b").expect_err("closed"),
            DslineError::Channel(ChannelError::Closed)
        );
        assert_eq!(ring.recv().expect("drain"), b"a");
        assert_eq!(
            ring.recv().expect_err("closed empty"),
            DslineError::Channel(ChannelError::Closed)
        );
    }

    #[test]
    fn sequence_wraps_without_panicking() {
        let mut ring = ring(2, 8);
        ring.next_seq = u64::MAX;
        ring.send(b"a").expect("send max");
        ring.send(b"b").expect("send wrapped");

        assert_eq!(ring.next_seq_for_test(), 1);
    }
}
