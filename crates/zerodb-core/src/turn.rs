//! Version 1 turn payloads. Length prefixes are little-endian u32 values.

use std::fmt;

/// The speaker's role.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Role {
    /// Instructions for the session.
    System,
    /// Input from a person.
    User,
    /// Agent output.
    Assistant,
    /// Tool output.
    Tool,
}

/// A turn before the writer assigns its sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewTurn {
    /// Speaker role.
    pub role: Role,
    /// Opaque content, at most 4 MiB.
    pub content: Vec<u8>,
    /// Opaque metadata, at most 64 KiB.
    pub meta: Vec<u8>,
}

/// A decoded tape payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedTurn {
    /// Agent identifier, at most 255 UTF-8 bytes.
    pub agent_id: String,
    /// Session identifier, at most 255 UTF-8 bytes.
    pub session_id: String,
    /// Writer-assigned sequence, starting at 1.
    pub seq: u64,
    /// Turn body.
    pub turn: NewTurn,
    /// Batch retry key, at most 128 UTF-8 bytes.
    pub idem_key: Option<String>,
}

/// A malformed or oversized turn payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CodecError;

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid turn payload or field limit")
    }
}
impl std::error::Error for CodecError {}

pub(crate) fn bytes(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
}

pub(crate) struct Cursor<'a>(pub(crate) &'a [u8]);
impl<'a> Cursor<'a> {
    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        let value = self.0.get(..n).ok_or(CodecError)?;
        self.0 = &self.0[n..];
        Ok(value)
    }
    pub(crate) fn byte(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }
    pub(crate) fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().map_err(|_| CodecError)?))
    }
    pub(crate) fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().map_err(|_| CodecError)?))
    }
    pub(crate) fn blob(&mut self, max: usize) -> Result<&'a [u8], CodecError> {
        let n = self.u32()? as usize;
        if n > max {
            return Err(CodecError);
        }
        self.take(n)
    }
    pub(crate) fn string(&mut self, max: usize) -> Result<String, CodecError> {
        String::from_utf8(self.blob(max)?.to_vec()).map_err(|_| CodecError)
    }
    pub(crate) fn end(self) -> Result<(), CodecError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(CodecError)
        }
    }
}

/// Encodes a turn. Callers pass sequence 0 before preparation.
///
/// The optional key uses a presence byte, followed by its length-prefixed UTF-8 bytes.
pub fn encode(
    agent: &str,
    session: &str,
    seq: u64,
    turn: &NewTurn,
    idem_key: Option<&str>,
) -> Result<Vec<u8>, CodecError> {
    if agent.len() > 255
        || session.len() > 255
        || turn.meta.len() > 65536
        || turn.content.len() > 4 * 1024 * 1024
        || idem_key.is_some_and(|key| key.len() > 128)
    {
        return Err(CodecError);
    }
    // Size the payload once. Growing it from one byte reallocates about ten times per turn, and
    // under glibc those cross-tcache reallocs contend on arena locks with the writer and readers.
    let len = 1
        + (4 + agent.len())
        + (4 + session.len())
        + 8
        + 1
        + (4 + turn.meta.len())
        + (4 + turn.content.len())
        + 1
        + idem_key.map_or(0, |key| 4 + key.len());
    let mut out = Vec::with_capacity(len);
    out.push(1);
    bytes(&mut out, agent.as_bytes());
    bytes(&mut out, session.as_bytes());
    out.extend_from_slice(&seq.to_le_bytes());
    out.push(turn.role as u8);
    bytes(&mut out, &turn.meta);
    bytes(&mut out, &turn.content);
    out.push(u8::from(idem_key.is_some()));
    if let Some(key) = idem_key {
        bytes(&mut out, key.as_bytes());
    }
    debug_assert_eq!(out.len(), len);
    Ok(out)
}

/// Decodes an exact payload, rejecting trailing bytes and unknown tags or versions.
pub fn decode(payload: &[u8]) -> Result<DecodedTurn, CodecError> {
    let parsed = decode_ref(payload)?;
    Ok(DecodedTurn {
        agent_id: parsed.agent_id.to_owned(),
        session_id: parsed.session_id.to_owned(),
        seq: parsed.seq,
        turn: NewTurn {
            role: parsed.role,
            content: parsed.content.to_vec(),
            meta: parsed.meta.to_vec(),
        },
        idem_key: parsed.idem_key.map(str::to_owned),
    })
}

pub(crate) struct ParsedTurn<'a> {
    pub(crate) agent_id: &'a str,
    pub(crate) session_id: &'a str,
    pub(crate) seq: u64,
    role: Role,
    content: &'a [u8],
    meta: &'a [u8],
    pub(crate) idem_key: Option<&'a str>,
}
pub(crate) fn decode_ref(payload: &[u8]) -> Result<ParsedTurn<'_>, CodecError> {
    let mut cur = Cursor(payload);
    if cur.byte()? != 1 {
        return Err(CodecError);
    }
    let agent_id = std::str::from_utf8(cur.blob(255)?).map_err(|_| CodecError)?;
    let session_id = std::str::from_utf8(cur.blob(255)?).map_err(|_| CodecError)?;
    let seq = cur.u64()?;
    let role = match cur.byte()? {
        0 => Role::System,
        1 => Role::User,
        2 => Role::Assistant,
        3 => Role::Tool,
        _ => return Err(CodecError),
    };
    let meta = cur.blob(65536)?;
    let content = cur.blob(4 * 1024 * 1024)?;
    let idem_key = match cur.byte()? {
        0 => None,
        1 => Some(std::str::from_utf8(cur.blob(128)?).map_err(|_| CodecError)?),
        _ => return Err(CodecError),
    };
    cur.end()?;
    Ok(ParsedTurn { agent_id, session_id, seq, role, content, meta, idem_key })
}

pub(crate) fn set_seq(payload: &mut [u8], seq: u64) -> Result<(), CodecError> {
    let mut cur = Cursor(payload);
    cur.take(1)?;
    cur.blob(255)?;
    cur.blob(255)?;
    let offset = payload.len() - cur.0.len();
    payload.get_mut(offset..offset + 8).ok_or(CodecError)?.copy_from_slice(&seq.to_le_bytes());
    Ok(())
}
