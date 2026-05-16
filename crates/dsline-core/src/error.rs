use std::fmt;

pub type Result<T> = std::result::Result<T, DslineError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DslineError {
    Channel(ChannelError),
    Transport(TransportError),
    Protocol(ProtocolError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelError {
    Closed,
    BufferFull,
    BufferEmpty,
    MessageTooLarge { len: usize, slot_size: usize },
    CorruptedMessage,
    SequenceMismatch { expected: u64, actual: u64 },
    InvalidConfig(&'static str),
    StorageIo(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    FrameTooShort { len: usize, minimum: usize },
    InvalidMagic(u32),
    UnsupportedVersion(u16),
    InvalidFrameKind(u16),
    InvalidHeaderLength { header_len: usize, minimum: usize },
    InvalidPayloadLength { declared: u64, available: usize },
    InvalidMetadataLength { offset: usize, declared: usize },
    HeaderLengthOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    EmptyUrl,
    MissingScheme,
    UnsupportedScheme(String),
    MissingTarget(&'static str),
    InvalidPort(String),
    InvalidQuery(String),
}

impl fmt::Display for DslineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Channel(err) => write!(f, "{err}"),
            Self::Transport(err) => write!(f, "{err}"),
            Self::Protocol(err) => write!(f, "{err}"),
        }
    }
}

impl fmt::Display for ChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "channel is closed"),
            Self::BufferFull => write!(f, "channel buffer is full"),
            Self::BufferEmpty => write!(f, "channel buffer is empty"),
            Self::MessageTooLarge { len, slot_size } => {
                write!(f, "message length {len} exceeds slot size {slot_size}")
            }
            Self::CorruptedMessage => write!(f, "message checksum validation failed"),
            Self::SequenceMismatch { expected, actual } => {
                write!(
                    f,
                    "message sequence mismatch: expected {expected}, got {actual}"
                )
            }
            Self::InvalidConfig(msg) => write!(f, "invalid channel config: {msg}"),
            Self::StorageIo(msg) => write!(f, "storage I/O error: {msg}"),
        }
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooShort { len, minimum } => {
                write!(f, "frame length {len} is shorter than minimum {minimum}")
            }
            Self::InvalidMagic(magic) => write!(f, "invalid frame magic 0x{magic:08x}"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported frame protocol version {version}")
            }
            Self::InvalidFrameKind(kind) => write!(f, "invalid frame kind {kind}"),
            Self::InvalidHeaderLength {
                header_len,
                minimum,
            } => write!(
                f,
                "invalid frame header length {header_len}; minimum is {minimum}"
            ),
            Self::InvalidPayloadLength {
                declared,
                available,
            } => write!(
                f,
                "invalid frame payload length {declared}; only {available} bytes available"
            ),
            Self::InvalidMetadataLength { offset, declared } => write!(
                f,
                "invalid metadata length {declared} at frame header offset {offset}"
            ),
            Self::HeaderLengthOverflow => write!(f, "frame header length overflow"),
        }
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyUrl => write!(f, "transport URL is empty"),
            Self::MissingScheme => write!(f, "transport URL is missing a scheme"),
            Self::UnsupportedScheme(scheme) => {
                write!(f, "unsupported transport scheme: {scheme}")
            }
            Self::MissingTarget(kind) => write!(f, "transport URL is missing {kind}"),
            Self::InvalidPort(port) => write!(f, "invalid transport port: {port}"),
            Self::InvalidQuery(query) => write!(f, "invalid transport query: {query}"),
        }
    }
}

impl std::error::Error for DslineError {}

impl From<ChannelError> for DslineError {
    fn from(value: ChannelError) -> Self {
        Self::Channel(value)
    }
}

impl From<ProtocolError> for DslineError {
    fn from(value: ProtocolError) -> Self {
        Self::Protocol(value)
    }
}

impl From<TransportError> for DslineError {
    fn from(value: TransportError) -> Self {
        Self::Transport(value)
    }
}
