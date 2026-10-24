//! [`HookIo`] wraps any [`Io`] so tests inject faults and observe calls with inline closures.

use crate::io::lock;
use crate::{Error, FileId, Io, LockGuard, Result, SimDisk};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// One I/O call as a hook sees it. File calls carry the path the file was opened with.
#[derive(Clone, Copy, Debug)]
#[allow(missing_docs)]
pub enum Op<'a> {
    Create(&'a Path),
    Open(&'a Path),
    Close(&'a Path),
    Write(&'a Path, FileId, u64, &'a [u8]),
    Read(&'a Path, u64, usize),
    Len(&'a Path),
    SetLen(&'a Path, u64),
    Allocate(&'a Path, u64),
    Available(&'a Path),
    Total(&'a Path),
    SyncData(&'a Path),
    SyncDir(&'a Path),
    Rename(&'a Path, &'a Path),
    Remove(&'a Path),
    List(&'a Path),
    Exists(&'a Path),
    Lock(&'a Path),
}

impl<'a> Op<'a> {
    /// The call's first path.
    pub fn path(&self) -> &'a Path {
        use Op::*;
        match *self {
            Create(p)
            | Open(p)
            | Close(p)
            | Write(p, ..)
            | Read(p, ..)
            | Len(p)
            | SetLen(p, _)
            | Allocate(p, _)
            | Available(p)
            | Total(p)
            | SyncData(p)
            | SyncDir(p)
            | Rename(p, _)
            | Remove(p)
            | List(p)
            | Exists(p)
            | Lock(p) => p,
        }
    }

    /// The first path's file name, or `""`.
    pub fn name(&self) -> &'a str {
        self.path().file_name().and_then(|n| n.to_str()).unwrap_or("")
    }

    /// True for a call on a tape segment file.
    pub fn is_segment(&self) -> bool {
        self.name().starts_with("seg-")
    }

    /// True for a call that can change storage.
    pub fn mutates(&self) -> bool {
        use Op::*;
        matches!(
            self,
            Create(_)
                | Write(..)
                | SetLen(..)
                | Allocate(..)
                | SyncData(_)
                | SyncDir(_)
                | Rename(..)
                | Remove(_)
        )
    }

    /// True for a file or directory sync.
    pub fn is_sync(&self) -> bool {
        matches!(self, Op::SyncData(_) | Op::SyncDir(_))
    }
}

/// Mutating calls print as `create|write|resize|allocate|sync|dir-sync|rename|remove <path> [arg]`.
/// Read-only calls print their variant name.
impl std::fmt::Display for Op<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let p = self.path().display();
        match *self {
            Op::Create(_) => write!(f, "create {p}"),
            Op::Write(_, _, off, _) => write!(f, "write {p} {off}"),
            Op::SetLen(_, n) => write!(f, "resize {p} {n}"),
            Op::Allocate(_, n) => write!(f, "allocate {p} {n}"),
            Op::SyncData(_) => write!(f, "sync {p}"),
            Op::SyncDir(_) => write!(f, "dir-sync {p}"),
            Op::Rename(_, to) => write!(f, "rename {p} {}", to.display()),
            Op::Remove(_) => write!(f, "remove {p}"),
            other => write!(f, "{other:?}"),
        }
    }
}

/// Runs the remaining hooks and then the real call. Returns the call's error, if any.
pub type Next<'n> = &'n mut dyn FnMut() -> Result<()>;
type Hook = dyn Fn(Op<'_>, Next<'_>) -> Result<()> + Send + Sync;

/// An [`Io`] that passes every call through a stack of hooks.
///
/// A hook gets the call and `next`. It can return an error without calling `next`,
/// call `next` and replace its result, or do work before and after it. A hook that
/// returns `Ok` without calling `next` fails the call, because no value exists.
/// The first hook added runs outermost, except [`HookIo::fail_once`] faults, which
/// run before every observer, so observers never see injected calls.
pub struct HookIo<I = SimDisk> {
    /// The wrapped storage.
    pub inner: I,
    files: Mutex<BTreeMap<FileId, PathBuf>>,
    hooks: Mutex<Vec<Arc<Hook>>>,
}

impl<I: Io> HookIo<I> {
    /// Wraps `inner` with no hooks.
    pub fn new(inner: I) -> Self {
        Self { inner, files: Mutex::default(), hooks: Mutex::default() }
    }

    /// Adds a hook, builder style.
    pub fn with(
        self,
        hook: impl Fn(Op<'_>, Next<'_>) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        self.add(hook);
        self
    }

    /// Adds a hook while the storage is in use.
    pub fn add(&self, hook: impl Fn(Op<'_>, Next<'_>) -> Result<()> + Send + Sync + 'static) {
        if let Ok(mut hooks) = lock(&self.hooks) {
            hooks.push(Arc::new(hook));
        }
    }

    /// Sums `f` over every later call, attempted or not. Starts at 0.
    pub fn tally(&self, f: impl Fn(&Op<'_>) -> u64 + Send + Sync + 'static) -> Arc<AtomicU64> {
        let total = Arc::new(AtomicU64::new(0));
        let sum = Arc::clone(&total);
        self.add(move |op, next| {
            sum.fetch_add(f(&op), Ordering::SeqCst);
            next()
        });
        total
    }

    /// Logs every later mutating call, after it runs, whatever its result.
    pub fn record(&self) -> Arc<Mutex<Vec<String>>> {
        let log = Arc::new(Mutex::new(Vec::new()));
        let out = Arc::clone(&log);
        self.add(move |op, next| {
            let result = next();
            if op.mutates() {
                lock(&out)?.push(op.to_string());
            }
            result
        });
        log
    }

    /// Number of files opened or created and not yet closed.
    pub fn open_files(&self) -> usize {
        lock(&self.files).map_or(0, |f| f.len())
    }

    /// Fails the next call that matches with an injected error of `kind`.
    pub fn fail_once(
        &self,
        kind: std::io::ErrorKind,
        matches: impl Fn(&Op<'_>) -> bool + Send + Sync + 'static,
    ) {
        let armed = AtomicBool::new(true);
        let hook: Arc<Hook> = Arc::new(move |op, next| {
            if matches(&op) && armed.swap(false, Ordering::SeqCst) {
                return Err(Error::Io(kind, format!("injected {op}")));
            }
            next()
        });
        if let Ok(mut hooks) = lock(&self.hooks) {
            hooks.insert(0, hook);
        }
    }

    fn path(&self, file: FileId) -> PathBuf {
        lock(&self.files).ok().and_then(|f| f.get(&file).cloned()).unwrap_or_default()
    }

    fn run<T>(&self, op: Op<'_>, mut call: impl FnMut() -> Result<T>) -> Result<T> {
        fn chain(hooks: &[Arc<Hook>], op: Op<'_>, last: Next<'_>) -> Result<()> {
            match hooks.split_first() {
                None => last(),
                Some((hook, rest)) => hook(op, &mut || chain(rest, op, &mut *last)),
            }
        }
        let hooks = lock(&self.hooks)?.clone();
        let mut out = None;
        chain(&hooks, op, &mut || {
            out = Some(call()?);
            Ok(())
        })?;
        out.ok_or_else(|| Error::Io(std::io::ErrorKind::Other, format!("hook skipped {op}")))
    }

    fn opened(&self, path: &Path, file: Result<FileId>) -> Result<FileId> {
        let file = file?;
        lock(&self.files)?.insert(file, path.to_owned());
        Ok(file)
    }
}

impl HookIo<SimDisk> {
    /// Wraps a fresh [`SimDisk`] seeded with `seed`.
    pub fn sim(seed: u64) -> Self {
        Self::new(SimDisk::new(seed))
    }

    /// Makes the `n`th later sync that `matches` fail as the disk would, counting from 1.
    pub fn fail_nth_sync(
        &self,
        n: usize,
        matches: impl Fn(&Op<'_>) -> bool + Send + Sync + 'static,
    ) {
        let (disk, seen) = (self.inner.clone(), AtomicUsize::new(0));
        self.add(move |op, next| {
            if op.is_sync() && matches(&op) && seen.fetch_add(1, Ordering::SeqCst) + 1 == n {
                disk.fail_next_sync()?;
            }
            next()
        });
    }

    /// Logs every later mutating call with a crash image taken right after it.
    pub fn capture(&self) -> Arc<Mutex<Vec<(String, SimDisk)>>> {
        let (disk, log) = (self.inner.clone(), Arc::new(Mutex::new(Vec::new())));
        let out = Arc::clone(&log);
        self.add(move |op, next| {
            let result = next();
            if op.mutates() {
                lock(&out)?.push((op.to_string(), disk.crash()?));
            }
            result
        });
        log
    }
}

impl<I> std::fmt::Debug for HookIo<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HookIo")
    }
}

impl<I: Io> Io for HookIo<I> {
    fn create(&self, p: &Path) -> Result<FileId> {
        self.opened(p, self.run(Op::Create(p), || self.inner.create(p)))
    }
    fn open(&self, p: &Path) -> Result<FileId> {
        self.opened(p, self.run(Op::Open(p), || self.inner.open(p)))
    }
    fn close(&self, f: FileId) -> Result<()> {
        self.run(Op::Close(&self.path(f)), || self.inner.close(f))?;
        lock(&self.files)?.remove(&f);
        Ok(())
    }
    fn write_at(&self, f: FileId, off: u64, d: &[u8]) -> Result<()> {
        self.run(Op::Write(&self.path(f), f, off, d), || self.inner.write_at(f, off, d))
    }
    fn read_at(&self, f: FileId, off: u64, d: &mut [u8]) -> Result<usize> {
        let path = self.path(f);
        let op = Op::Read(&path, off, d.len());
        self.run(op, || self.inner.read_at(f, off, d))
    }
    fn len(&self, f: FileId) -> Result<u64> {
        self.run(Op::Len(&self.path(f)), || self.inner.len(f))
    }
    fn set_len(&self, f: FileId, n: u64) -> Result<()> {
        self.run(Op::SetLen(&self.path(f), n), || self.inner.set_len(f, n))
    }
    fn allocate(&self, f: FileId, n: u64) -> Result<()> {
        self.run(Op::Allocate(&self.path(f), n), || self.inner.allocate(f, n))
    }
    fn available_bytes(&self, p: &Path) -> Result<u64> {
        self.run(Op::Available(p), || self.inner.available_bytes(p))
    }
    fn total_bytes(&self, p: &Path) -> Result<u64> {
        self.run(Op::Total(p), || self.inner.total_bytes(p))
    }
    fn sync_data(&self, f: FileId) -> Result<()> {
        self.run(Op::SyncData(&self.path(f)), || self.inner.sync_data(f))
    }
    fn sync_dir(&self, p: &Path) -> Result<()> {
        self.run(Op::SyncDir(p), || self.inner.sync_dir(p))
    }
    fn rename(&self, a: &Path, b: &Path) -> Result<()> {
        self.run(Op::Rename(a, b), || self.inner.rename(a, b))
    }
    fn remove(&self, p: &Path) -> Result<()> {
        self.run(Op::Remove(p), || self.inner.remove(p))
    }
    fn list(&self, p: &Path) -> Result<Vec<PathBuf>> {
        self.run(Op::List(p), || self.inner.list(p))
    }
    fn exists(&self, p: &Path) -> Result<bool> {
        self.run(Op::Exists(p), || self.inner.exists(p))
    }
    fn try_lock_exclusive(&self, p: &Path) -> Result<LockGuard> {
        self.run(Op::Lock(p), || self.inner.try_lock_exclusive(p))
    }
}
