use crate::error::Result;
use crate::{EngineError, ErrorKind};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Key(pub(crate) u32, pub(crate) String, pub(crate) String);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Backing {
    pub(crate) file: String,
    pub(crate) offset: u64,
    pub(crate) len: u32,
}

#[derive(Debug)]
pub(crate) struct Page {
    pub(crate) slots: Option<Arc<Vec<u64>>>,
    pub(crate) backing: Option<Backing>,
    pub(crate) revision: u64,
    pub(crate) dirty: bool,
    pub(crate) used: usize,
    touch: u64,
}

#[derive(Debug, Default)]
pub(crate) struct SessionEntry {
    pub(crate) len: u64,
    pub(crate) reserved: u64,
    pub(crate) pages: Vec<Page>,
    pub(crate) pins: usize,
}

/// Geometric pages repeat 4096-slot pages after the first 1360 slots.
pub(crate) fn address(seq: u64) -> (usize, usize) {
    let offset = seq - 1;
    let mut base = 0;
    for (page, cap) in [16, 64, 256, 1024].into_iter().enumerate() {
        if offset < base + cap {
            return (page, (offset - base) as usize);
        }
        base += cap;
    }
    (4 + ((offset - base) / 4096) as usize, ((offset - base) % 4096) as usize)
}

pub(crate) fn capacity(page: usize) -> usize {
    [16, 64, 256, 1024].get(page).copied().unwrap_or(4096)
}

#[derive(Debug)]
pub(crate) struct Index {
    pub(crate) sessions: HashMap<Key, SessionEntry>,
    pub(crate) hot_bytes: usize,
    pub(crate) dirty_bytes: usize,
    pub(crate) budget: usize,
    pub(crate) max_sessions: usize,
    pub(crate) max_pages: usize,
    pages: usize,
    clock: u64,
    lru: BTreeSet<(u64, Key, usize)>,
}

fn exhausted(resource: &'static str) -> EngineError {
    EngineError::rejected(ErrorKind::ResourceExhausted(resource))
}

impl Index {
    pub(crate) fn new(budget: usize, max_sessions: usize, max_pages: usize) -> Self {
        Self {
            sessions: HashMap::new(),
            hot_bytes: 0,
            dirty_bytes: 0,
            budget,
            max_sessions,
            max_pages,
            pages: 0,
            clock: 0,
            lru: BTreeSet::new(),
        }
    }

    fn touch(&mut self, key: &Key, page: usize) {
        let p = &mut self.sessions.get_mut(key).unwrap_or_else(|| unreachable!()).pages[page];
        self.lru.remove(&(p.touch, key.clone(), page));
        self.clock = self.clock.saturating_add(1);
        p.touch = self.clock;
        if p.slots.is_some() && !p.dirty && p.backing.is_some() {
            self.lru.insert((p.touch, key.clone(), page));
        }
    }

    fn room(&mut self, bytes: usize) -> Result<()> {
        let mut pinned = Vec::new();
        while self.hot_bytes.saturating_add(bytes) > self.budget {
            let Some(item) = self.lru.pop_first() else {
                self.lru.extend(pinned);
                return Err(exhausted("hot_pages"));
            };
            let entry = self.sessions.get_mut(&item.1).unwrap_or_else(|| unreachable!());
            let pending = entry.reserved > entry.len
                && item.2 >= address(entry.len + 1).0
                && item.2 <= address(entry.reserved).0;
            let pinned_tail = entry.pins > 0
                && entry.reserved > 0
                && item.2 == address(entry.reserved).0
                && entry.pages[item.2]
                    .slots
                    .as_ref()
                    .is_some_and(|slots| slots.len() < capacity(item.2));
            if pending || pinned_tail {
                pinned.push(item);
                continue;
            }
            if entry.pages[item.2].slots.take().is_some() {
                self.hot_bytes -= capacity(item.2) * 8;
            }
        }
        self.lru.extend(pinned);
        Ok(())
    }

    pub(crate) fn reserve(&mut self, key: &Key, count: usize) -> Result<u64> {
        let entry = self.sessions.get(key);
        if entry.is_none() && self.sessions.len() >= self.max_sessions {
            return Err(exhausted("sessions"));
        }
        let old = entry.map_or(0, |e| e.reserved);
        let end = old
            .checked_add(count as u64)
            .filter(|&seq| seq < u64::MAX)
            .ok_or_else(|| exhausted("sequences"))?;
        let needed = address(end).0 + 1;
        let existing = entry.map_or(0, |e| e.pages.len());
        if self.pages.saturating_add(needed - existing) > self.max_pages {
            return Err(exhausted("page_directory"));
        }
        if old > 0 && entry.is_some_and(|e| e.pages[address(old).0].slots.is_none()) {
            return Err(exhausted("hot_pages"));
        }
        let bytes = (existing..needed).map(|p| capacity(p) * 8).sum();
        self.room(bytes)?;
        let entry = self.sessions.entry(key.clone()).or_default();
        for page in existing..needed {
            entry.pages.push(Page {
                slots: Some(Arc::new(Vec::with_capacity(capacity(page)))),
                backing: None,
                revision: 0,
                dirty: true,
                used: 0,
                touch: 0,
            });
        }
        entry.reserved = end;
        self.pages += needed - existing;
        self.hot_bytes += bytes;
        self.dirty_bytes += bytes;
        Ok(old + 1)
    }

    pub(crate) fn publish(&mut self, key: &Key, seq: u64, lsn: u64) -> Result<()> {
        let entry = self
            .sessions
            .get_mut(key)
            .ok_or_else(|| EngineError::rejected(ErrorKind::CorruptIndex))?;
        if seq != entry.len + 1 {
            return Err(EngineError::rejected(ErrorKind::CorruptIndex));
        }
        let (page, slot) = address(seq);
        let p = entry
            .pages
            .get_mut(page)
            .ok_or_else(|| EngineError::rejected(ErrorKind::CorruptIndex))?;
        let slots = Arc::make_mut(p.slots.as_mut().ok_or_else(|| exhausted("hot_pages"))?);
        if slots.len() != slot {
            return Err(EngineError::rejected(ErrorKind::CorruptIndex));
        }
        if slots.capacity() < capacity(page) {
            slots.reserve_exact(capacity(page) - slots.len());
        }
        self.lru.remove(&(p.touch, key.clone(), page));
        slots.push(lsn);
        p.used = slots.len();
        p.revision = lsn;
        if !p.dirty {
            self.dirty_bytes += capacity(page) * 8;
        }
        p.dirty = true;
        entry.len = seq;
        Ok(())
    }

    pub(crate) fn resident(&mut self, key: &Key, page: usize) -> Result<Option<Arc<Vec<u64>>>> {
        let slots = self
            .sessions
            .get(key)
            .and_then(|entry| entry.pages.get(page))
            .ok_or_else(|| EngineError::rejected(ErrorKind::CorruptIndex))?
            .slots
            .clone();
        if slots.is_some() {
            self.touch(key, page);
        }
        Ok(slots)
    }

    pub(crate) fn backing(&self, key: &Key, page: usize) -> Result<Backing> {
        self.sessions
            .get(key)
            .and_then(|entry| entry.pages.get(page))
            .and_then(|page| page.backing.clone())
            .ok_or_else(|| EngineError::rejected(ErrorKind::CorruptIndex))
    }

    pub(crate) fn install_loaded(
        &mut self,
        key: &Key,
        page: usize,
        backing: &Backing,
        slots: Vec<u64>,
    ) -> Result<bool> {
        let p = self
            .sessions
            .get(key)
            .and_then(|entry| entry.pages.get(page))
            .ok_or_else(|| EngineError::rejected(ErrorKind::CorruptIndex))?;
        if p.slots.is_some() {
            return Ok(true);
        }
        if p.backing.as_ref() != Some(backing) {
            return Ok(false);
        }
        if slots.len() != p.used || slots.last().copied() != Some(p.revision) {
            return Err(EngineError::rejected(ErrorKind::CorruptIndex));
        }
        self.room(capacity(page) * 8)?;
        let mut hot = Vec::with_capacity(capacity(page));
        hot.extend(slots);
        self.sessions.get_mut(key).unwrap_or_else(|| unreachable!()).pages[page].slots =
            Some(Arc::new(hot));
        self.hot_bytes += capacity(page) * 8;
        self.touch(key, page);
        Ok(true)
    }

    pub(crate) fn load(
        &mut self,
        key: &Key,
        page: usize,
        read: &mut impl FnMut(&Backing) -> Result<Vec<u64>>,
    ) -> Result<()> {
        if self.resident(key, page)?.is_none() {
            let backing = self.backing(key, page)?;
            self.install_loaded(key, page, &backing, read(&backing)?)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn window(
        &mut self,
        key: &Key,
        start: u64,
        end: u64,
        mut read: impl FnMut(&Backing) -> Result<Vec<u64>>,
    ) -> Result<Vec<u64>> {
        let mut out = Vec::new();
        let mut seq = start;
        while seq < end {
            let (page, slot) = address(seq);
            self.load(key, page, &mut read)?;
            let slots =
                self.sessions[key].pages[page].slots.as_ref().unwrap_or_else(|| unreachable!());
            let available = slots
                .len()
                .checked_sub(slot)
                .ok_or_else(|| EngineError::rejected(ErrorKind::CorruptIndex))?;
            let count = (end - seq).min(available as u64) as usize;
            if count == 0 {
                return Err(EngineError::rejected(ErrorKind::CorruptIndex));
            }
            out.extend_from_slice(&slots[slot..slot + count]);
            seq += count as u64;
        }
        Ok(out)
    }

    pub(crate) fn clean(&mut self, key: &Key, page: usize, revision: u64, backing: Backing) {
        let p = &mut self.sessions.get_mut(key).unwrap_or_else(|| unreachable!()).pages[page];
        p.backing = Some(backing);
        if p.revision == revision && p.dirty {
            p.dirty = false;
            self.dirty_bytes -= capacity(page) * 8;
        }
        self.touch(key, page);
    }

    pub(crate) fn restore(
        &mut self,
        key: Key,
        page: usize,
        count: usize,
        revision: u64,
        backing: Backing,
    ) -> Result<()> {
        if count == 0 || count > capacity(page) {
            return Err(EngineError::rejected(ErrorKind::CorruptIndex));
        }
        if !self.sessions.contains_key(&key) && self.sessions.len() >= self.max_sessions {
            return Err(exhausted("sessions"));
        }
        let entry = self.sessions.entry(key).or_default();
        if page > entry.pages.len()
            || (page == entry.pages.len()
                && page > 0
                && entry.pages[page - 1].used != capacity(page - 1))
            || entry.pages.get(page).is_some_and(|old| count < old.used || revision < old.revision)
            || (page + 1 < entry.pages.len() && count != capacity(page))
        {
            return Err(EngineError::rejected(ErrorKind::CorruptIndex));
        }
        let p = Page {
            slots: None,
            backing: Some(backing),
            revision,
            dirty: false,
            used: count,
            touch: 0,
        };
        if page == entry.pages.len() {
            if self.pages >= self.max_pages {
                return Err(exhausted("page_directory"));
            }
            entry.pages.push(p);
            self.pages += 1;
        } else {
            entry.pages[page] = p;
        }
        let base = if page < 4 { [0, 16, 80, 336][page] } else { 1360 + (page as u64 - 4) * 4096 };
        entry.len = entry.len.max(base + count as u64);
        entry.reserved = entry.len;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn short_resident_page_returns_corruption_instead_of_panicking() {
        let key = Key(1, "a".into(), "s".into());
        let mut index = Index::new(4096, 10, 100);
        index.reserve(&key, 17).unwrap();
        index.publish(&key, 1, 2).unwrap();
        assert_eq!(index.window(&key, 1, 2, |_| unreachable!()).unwrap(), vec![2]);
        index.sessions.get_mut(&key).unwrap().len = 17;
        let error = index.window(&key, 10, 11, |_| unreachable!()).unwrap_err();
        assert!(matches!(error.kind, ErrorKind::CorruptIndex));
    }
}
