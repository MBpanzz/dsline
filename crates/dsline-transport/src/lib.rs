//! Transport URL parsing and transport trait.
//!
//! Transport implementations are deferred until the SPSC shared-memory channel
//! has a tested storage backend. This crate provides the URL grammar and trait
//! that all transports share.

use dsline_core::error::{Result, TransportError};
use std::fmt;
use std::str::FromStr;

/// Transport schemes that dsline recognizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportScheme {
    /// Shared-memory channel: `shm://name?capacity=1024&slot_size=4096`
    Shm,
    /// Bus transport (multi-consumer broadcast): `bus://topic`
    Bus,
    /// Unix domain socket: `unix:///path/to/socket`
    Unix,
    /// TCP socket: `tcp://host:port/topic`
    Tcp,
}

impl TransportScheme {
    /// Returns the scheme prefix as it appears in a URL.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Shm => "shm",
            Self::Bus => "bus",
            Self::Unix => "unix",
            Self::Tcp => "tcp",
        }
    }
}

impl fmt::Display for TransportScheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TransportScheme {
    type Err = dsline_core::error::DslineError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "shm" => Ok(Self::Shm),
            "bus" => Ok(Self::Bus),
            "unix" => Ok(Self::Unix),
            "tcp" => Ok(Self::Tcp),
            other => Err(TransportError::UnsupportedScheme(other.to_owned()).into()),
        }
    }
}

/// A parsed transport URL.
///
/// Format: `scheme://target?key1=value1&key2=value2`
///
/// # Examples
///
/// ```
/// use dsline_transport::TransportUrl;
///
/// let url: TransportUrl = "shm://demo?capacity=4&slot_size=64".parse().unwrap();
/// assert_eq!(url.scheme.as_str(), "shm");
/// assert_eq!(url.target(), "demo");
/// assert_eq!(url.query_param("capacity"), Some("4"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportUrl {
    pub scheme: TransportScheme,
    target: String,
    query: Vec<(String, String)>,
}

impl TransportUrl {
    /// The target portion of the URL (between `://` and `?` or end of string).
    pub fn target(&self) -> &str {
        &self.target
    }

    /// Look up a query parameter by name. Returns `None` if absent.
    ///
    /// If a key appears multiple times, the first occurrence wins.
    pub fn query_param(&self, key: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// All query parameters in order of appearance.
    pub fn query_pairs(&self) -> &[(String, String)] {
        &self.query
    }

    /// Reconstruct the URL string.
    pub fn to_url_string(&self) -> String {
        let mut s = format!("{}://{}", self.scheme, self.target);
        for (i, (k, v)) in self.query.iter().enumerate() {
            s.push(if i == 0 { '?' } else { '&' });
            s.push_str(k);
            s.push('=');
            s.push_str(v);
        }
        s
    }
}

impl fmt::Display for TransportUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_url_string())
    }
}

impl FromStr for TransportUrl {
    type Err = dsline_core::error::DslineError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(TransportError::EmptyUrl.into());
        }

        let (scheme_part, rest) = s.split_once("://").ok_or(TransportError::MissingScheme)?;
        if scheme_part.is_empty() {
            return Err(TransportError::MissingScheme.into());
        }

        let scheme: TransportScheme = scheme_part.parse()?;

        let (target, query_part) = match rest.split_once('?') {
            Some((t, q)) => (t, q),
            None => (rest, ""),
        };

        if target.is_empty() {
            let kind = match scheme {
                TransportScheme::Shm => "channel name",
                TransportScheme::Bus => "topic",
                TransportScheme::Unix => "socket path",
                TransportScheme::Tcp => "host:port",
            };
            return Err(TransportError::MissingTarget(kind).into());
        }

        // Validate TCP port early if present.
        if scheme == TransportScheme::Tcp {
            validate_tcp_target(target)?;
        }

        let query = if query_part.is_empty() {
            Vec::new()
        } else {
            parse_query(query_part)?
        };

        Ok(Self {
            scheme,
            target: target.to_owned(),
            query,
        })
    }
}

fn validate_tcp_target(target: &str) -> Result<()> {
    // Accept host:port or host:port/path. The port is the numeric segment
    // between the last colon and the next slash (or end of string).
    let colon = target
        .rfind(':')
        .ok_or(TransportError::MissingTarget("host:port"))?;
    let host = &target[..colon];
    let port_and_path = &target[colon + 1..];

    let port_str = match port_and_path.find('/') {
        Some(slash) => &port_and_path[..slash],
        None => port_and_path,
    };

    if host.is_empty() {
        return Err(TransportError::MissingTarget("host").into());
    }
    if port_str.is_empty() || port_str.parse::<u16>().is_err() {
        return Err(TransportError::InvalidPort(port_str.to_owned()).into());
    }
    Ok(())
}

fn parse_query(query: &str) -> Result<Vec<(String, String)>> {
    let mut pairs = Vec::new();
    for part in query.split('&') {
        if part.is_empty() {
            continue;
        }
        let (k, v) = part
            .split_once('=')
            .ok_or_else(|| TransportError::InvalidQuery(part.to_owned()))?;
        if k.is_empty() {
            return Err(TransportError::InvalidQuery(part.to_owned()).into());
        }
        pairs.push((k.to_owned(), v.to_owned()));
    }
    Ok(pairs)
}

/// Common trait for transport-level send/receive endpoints.
///
/// Each transport backend (shm, bus, unix, tcp) implements this trait so that
/// the pipeline layer can treat them uniformly.
pub trait Transport {
    /// The scheme this transport speaks.
    fn scheme(&self) -> TransportScheme;

    /// The URL this endpoint was created from.
    fn url(&self) -> &TransportUrl;

    /// A human-readable label for logs and metrics.
    fn label(&self) -> String {
        self.url().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{TransportScheme, TransportUrl};
    use dsline_core::error::{DslineError, TransportError};
    use std::str::FromStr;

    // ── scheme ──

    #[test]
    fn parse_supported_schemes() {
        assert_eq!(
            TransportScheme::from_str("shm").unwrap(),
            TransportScheme::Shm
        );
        assert_eq!(
            TransportScheme::from_str("bus").unwrap(),
            TransportScheme::Bus
        );
        assert_eq!(
            TransportScheme::from_str("unix").unwrap(),
            TransportScheme::Unix
        );
        assert_eq!(
            TransportScheme::from_str("tcp").unwrap(),
            TransportScheme::Tcp
        );
    }

    #[test]
    fn reject_unsupported_scheme() {
        assert_eq!(
            TransportScheme::from_str("kafka").unwrap_err(),
            DslineError::Transport(TransportError::UnsupportedScheme("kafka".into()))
        );
    }

    #[test]
    fn scheme_display_round_trips() {
        for scheme in ["shm", "bus", "unix", "tcp"] {
            let parsed: TransportScheme = scheme.parse().unwrap();
            assert_eq!(parsed.as_str(), scheme);
            assert_eq!(parsed.to_string(), scheme);
        }
    }

    // ── url ──

    #[test]
    fn parse_minimal_url() {
        let url: TransportUrl = "shm://demo".parse().unwrap();
        assert_eq!(url.scheme, TransportScheme::Shm);
        assert_eq!(url.target(), "demo");
        assert!(url.query_pairs().is_empty());
    }

    #[test]
    fn parse_url_with_query() {
        let url: TransportUrl = "shm://demo?capacity=4&slot_size=64".parse().unwrap();
        assert_eq!(url.target(), "demo");
        assert_eq!(url.query_param("capacity"), Some("4"));
        assert_eq!(url.query_param("slot_size"), Some("64"));
        assert_eq!(url.query_param("missing"), None);
    }

    #[test]
    fn parse_bus_url() {
        let url: TransportUrl = "bus://sensors".parse().unwrap();
        assert_eq!(url.scheme, TransportScheme::Bus);
        assert_eq!(url.target(), "sensors");
    }

    #[test]
    fn parse_unix_url() {
        let url: TransportUrl = "unix:///tmp/dsline.sock".parse().unwrap();
        assert_eq!(url.scheme, TransportScheme::Unix);
        assert_eq!(url.target(), "/tmp/dsline.sock");
    }

    #[test]
    fn parse_tcp_url() {
        let url: TransportUrl = "tcp://127.0.0.1:9000/topic".parse().unwrap();
        assert_eq!(url.scheme, TransportScheme::Tcp);
        assert_eq!(url.target(), "127.0.0.1:9000/topic");
    }

    #[test]
    fn parse_tcp_url_with_query() {
        let url: TransportUrl = "tcp://localhost:8080/stream?retry=3".parse().unwrap();
        assert_eq!(url.target(), "localhost:8080/stream");
        assert_eq!(url.query_param("retry"), Some("3"));
    }

    #[test]
    fn reject_empty_url() {
        assert_eq!(
            "".parse::<TransportUrl>().unwrap_err(),
            DslineError::Transport(TransportError::EmptyUrl)
        );
    }

    #[test]
    fn reject_missing_scheme() {
        assert_eq!(
            "demo".parse::<TransportUrl>().unwrap_err(),
            DslineError::Transport(TransportError::MissingScheme)
        );
    }

    #[test]
    fn reject_scheme_without_separator() {
        assert_eq!(
            "shm:demo".parse::<TransportUrl>().unwrap_err(),
            DslineError::Transport(TransportError::MissingScheme)
        );
    }

    #[test]
    fn reject_missing_target() {
        assert_eq!(
            "shm://".parse::<TransportUrl>().unwrap_err(),
            DslineError::Transport(TransportError::MissingTarget("channel name"))
        );
        assert_eq!(
            "bus://".parse::<TransportUrl>().unwrap_err(),
            DslineError::Transport(TransportError::MissingTarget("topic"))
        );
        assert_eq!(
            "unix://".parse::<TransportUrl>().unwrap_err(),
            DslineError::Transport(TransportError::MissingTarget("socket path"))
        );
        assert_eq!(
            "tcp://".parse::<TransportUrl>().unwrap_err(),
            DslineError::Transport(TransportError::MissingTarget("host:port"))
        );
    }

    #[test]
    fn reject_tcp_missing_port() {
        assert_eq!(
            "tcp://localhost".parse::<TransportUrl>().unwrap_err(),
            DslineError::Transport(TransportError::MissingTarget("host:port"))
        );
    }

    #[test]
    fn reject_tcp_invalid_port() {
        assert_eq!(
            "tcp://host:abc".parse::<TransportUrl>().unwrap_err(),
            DslineError::Transport(TransportError::InvalidPort("abc".into()))
        );
        assert_eq!(
            "tcp://host:99999".parse::<TransportUrl>().unwrap_err(),
            DslineError::Transport(TransportError::InvalidPort("99999".into()))
        );
    }

    #[test]
    fn reject_invalid_query() {
        assert_eq!(
            "shm://demo?badquery".parse::<TransportUrl>().unwrap_err(),
            DslineError::Transport(TransportError::InvalidQuery("badquery".into()))
        );
        assert_eq!(
            "shm://demo?=value".parse::<TransportUrl>().unwrap_err(),
            DslineError::Transport(TransportError::InvalidQuery("=value".into()))
        );
    }

    #[test]
    fn url_display_round_trips() {
        for input in [
            "shm://demo",
            "shm://demo?capacity=4&slot_size=64",
            "bus://sensors",
            "unix:///tmp/dsline.sock",
            "tcp://127.0.0.1:9000/topic",
            "tcp://localhost:8080/stream?retry=3",
        ] {
            let url: TransportUrl = input.parse().unwrap();
            assert_eq!(url.to_string(), input);
        }
    }

    #[test]
    fn query_param_first_wins_on_duplicate() {
        let url: TransportUrl = "shm://demo?a=1&a=2".parse().unwrap();
        assert_eq!(url.query_param("a"), Some("1"));
    }
}
