use crate::error::Result;
use crate::{EngineError, ErrorKind};
use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub(crate) struct Registry {
    names: HashMap<String, u32>,
    pub(crate) entries: Vec<(String, u64)>,
}

impl Registry {
    pub(crate) fn get(&self, name: &str) -> Option<u32> {
        self.names.get(name).copied()
    }
    pub(crate) fn insert(&mut self, name: &str, id: u32, lsn: u64, limit: usize) -> Result<()> {
        if name.len() > 255
            || id as usize != self.entries.len() + 1
            || self.names.contains_key(name)
        {
            return Err(EngineError::rejected(ErrorKind::CorruptIndex));
        }
        if self.entries.len() >= limit.min(10_000) {
            return Err(EngineError::rejected(ErrorKind::ResourceExhausted("tenants")));
        }
        self.names.insert(name.to_owned(), id);
        self.entries.push((name.to_owned(), lsn));
        Ok(())
    }
}
