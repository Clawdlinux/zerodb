use crate::support::*;
use std::path::Path;
use std::sync::{Arc, Mutex};
use zerodb_tape::sim::{atomic_install, make_spare, DataDir};
use zerodb_tape::{Error, HookIo, Io, SimDisk, StdIo};

#[test]
fn filesystem_operations_through_trait_object() {
    let dir = TestDir::new();
    let io: Arc<dyn Io> = Arc::new(StdIo::new());
    let path = dir.0.join("data");
    assert!(!io.exists(&path).unwrap());
    let file = io.create(&path).unwrap();
    assert!(io.available_bytes(&dir.0).unwrap() <= io.total_bytes(&dir.0).unwrap());
    io.allocate(file, 32).unwrap();
    io.write_at(file, 4, b"hello").unwrap();
    io.sync_data(file).unwrap();
    io.sync_dir(&dir.0).unwrap();
    assert_eq!(io.len(file).unwrap(), 32);
    let reopened = io.open(&path).unwrap();
    let mut bytes = [0; 8];
    assert_eq!(io.read_at(reopened, 4, &mut bytes).unwrap(), 8);
    assert_eq!(&bytes, b"hello\0\0\0");
    assert_eq!(io.read_at(file, 32, &mut bytes).unwrap(), 0);
    let renamed = dir.0.join("renamed");
    io.rename(&path, &renamed).unwrap();
    assert_eq!(io.list(&dir.0).unwrap(), vec![renamed.clone()]);
    io.set_len(file, 5).unwrap();
    assert_eq!(io.len(reopened).unwrap(), 5);
    io.remove(&renamed).unwrap();
    assert!(!io.exists(&renamed).unwrap());
    assert!(matches!(io.open(&renamed), Err(Error::Io(_, _))));

    let path = dir.0.join("LOCK");
    let first = StdIo::new();
    let file = first.create(&path).unwrap();
    first.write_at(file, 0, b"PID 123 server local").unwrap();
    let guard = first.try_lock_exclusive(&path).unwrap();
    let second = StdIo::new();
    match second.try_lock_exclusive(&path) {
        Err(Error::DataDirLocked { holder }) => assert_eq!(holder, "PID 123 server local"),
        result => panic!("expected lock contention, got {result:?}"),
    }
    drop(guard);
    assert!(second.try_lock_exclusive(&path).is_ok());
}

#[cfg(target_os = "linux")]
fn preallocation_supported(dir: &Path) -> bool {
    use rustix::io::Errno;
    let path = dir.join("preallocation-probe");
    let file =
        std::fs::OpenOptions::new().create_new(true).read(true).write(true).open(&path).unwrap();
    let allocation =
        rustix::fs::fallocate(&file, rustix::fs::FallocateFlags::empty(), 0, 1024 * 1024);
    drop(file);
    std::fs::remove_file(path).unwrap();
    match allocation {
        Ok(()) => true,
        Err(error) if [Errno::OPNOTSUPP, Errno::NOTSUP, Errno::NOSYS].contains(&error) => {
            eprintln!("skipping allocation block check: preallocation unsupported: {error}");
            false
        }
        Err(error) => panic!("preallocation probe failed: {error}"),
    }
}

#[test]
fn allocation_sets_length_for_growth_repeats_and_shrink() {
    let dir = TestDir::new();
    #[cfg(target_os = "linux")]
    let preallocation_supported = preallocation_supported(&dir.0);
    let io = StdIo::new();
    let path = dir.0.join("resized");
    let file = io.create(&path).unwrap();
    let metadata_file = std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
    for (case, len, expect_reserved) in [
        ("empty file", 0, false),
        ("initial reservation", 1024 * 1024, true),
        ("repeated reservation", 1024 * 1024, false),
        ("shrink", 512 * 1024, false),
        ("growth", 2 * 1024 * 1024, false),
        ("repeated growth", 2 * 1024 * 1024, false),
        ("second growth", 3 * 1024 * 1024, false),
        ("truncate", 0, false),
    ] {
        #[cfg(target_os = "macos")]
        let before = {
            metadata_file.sync_all().unwrap();
            metadata_file.metadata().unwrap()
        };
        io.allocate(file, len).unwrap();
        assert_eq!(io.len(file).unwrap(), len, "{case}: length={len}");
        if expect_reserved {
            io.sync_data(file).unwrap();
        }
        metadata_file.sync_all().unwrap();
        let metadata = metadata_file.metadata().unwrap();
        assert_eq!(metadata.len(), len, "{case}: length={len}");
        // APFS reported 704 to 1696 of 2048 blocks for a 1 MiB F_PREALLOCATE when other tests
        // allocated in parallel, and all 2048 in isolation. Only Linux checks the lower bound.
        #[cfg(target_os = "linux")]
        if expect_reserved && preallocation_supported {
            use std::os::unix::fs::MetadataExt;
            assert!(metadata.blocks() * 512 >= len, "{case}: length={len}");
        }
        #[cfg(target_os = "macos")]
        {
            use std::os::unix::fs::MetadataExt;
            let used = metadata.blocks() * 512;
            let extra = len != 0 && len <= before.len() && used >= before.len() + len / 2;
            assert!(!extra, "{case}: length={len} must not reserve a whole extra length");
            // Growth must not reserve the target length again past the physical end.
            let limit = len.max(before.blocks() * 512) + len / 8;
            assert!(used <= limit, "{case}: length={len} used={used} over-reserves");
        }
    }
    io.close(file).unwrap();
}

#[test]
fn backends_agree_on_closed_handles_listing_and_existence() {
    let dir = TestDir::new();
    let (branch, direct) = (dir.0.join("branch"), dir.0.join("direct"));
    let deeper = branch.join("deeper");
    let leaf = deeper.join("leaf");
    std::fs::create_dir_all(&deeper).unwrap();
    let backends: [(&str, Box<dyn Io>); 2] =
        [("filesystem", Box::new(StdIo::new())), ("simulated", Box::new(SimDisk::new(0)))];
    for (case, io) in backends {
        let path = dir.0.join("closed");
        let file = io.create(&path).unwrap();
        io.write_at(file, 0, b"data").unwrap();
        let other = io.open(&path).unwrap();
        assert_ne!(file, other, "{case}");
        io.close(file).unwrap();
        assert!(io.read_at(file, 0, &mut [0; 4]).is_err(), "{case}");
        assert!(io.write_at(file, 0, b"bad").is_err(), "{case}");
        assert!(io.len(file).is_err(), "{case}");
        assert!(io.set_len(file, 0).is_err(), "{case}");
        assert!(io.sync_data(file).is_err(), "{case}");
        assert!(io.close(file).is_err(), "{case}");
        assert_eq!(io.len(other).unwrap(), 4, "{case}");
        io.remove(&path).unwrap();
        assert_eq!(io.len(other).unwrap(), 4, "{case}: unlinked file stays open");
        io.close(other).unwrap();
        for path in [&direct, &leaf] {
            io.close(io.create(path).unwrap()).unwrap();
            assert!(io.create(path).is_err(), "{case}: create never replaces {path:?}");
        }
        for path in [&dir.0, &branch, &deeper, &leaf] {
            assert!(io.exists(path).unwrap(), "{case}: {path:?}");
        }
        assert!(!io.exists(&branch.join("missing")).unwrap(), "{case}");
        assert_eq!(io.list(&dir.0).unwrap(), vec![branch.clone(), direct.clone()], "{case}");
        assert_eq!(io.list(&branch).unwrap(), vec![deeper.clone()], "{case}");
        assert_eq!(io.list(&deeper).unwrap(), vec![leaf.clone()], "{case}");
        assert!(io.list(&leaf).is_err() && io.list(&branch.join("missing")).is_err(), "{case}");
        let moved = branch.join("moved");
        io.rename(&direct, &moved).unwrap();
        io.sync_dir(&branch).unwrap();
        assert_eq!(io.list(&dir.0).unwrap(), vec![branch.clone()], "{case}: renamed away");
        assert_eq!(io.list(&branch).unwrap(), vec![deeper.clone(), moved.clone()], "{case}");
        io.remove(&moved).unwrap();
        io.remove(&leaf).unwrap();
    }
}

#[test]
fn manifest_round_trips_and_damage_is_corruption() {
    let io = Arc::new(SimDisk::new(1));
    let mut dir = DataDir::open(io.clone(), "db", Some("localhost:77"), 1024).unwrap();
    dir.prepare_next_segment(1024).unwrap();
    dir.install_manifest(&[1, 2]).unwrap();
    dir.install_superblock(99).unwrap();
    drop(dir);
    let dir = DataDir::open(io.clone(), "db", None, 1024).unwrap();
    assert_eq!((dir.segments(), dir.superblock(), dir.segment_size()), (&[1, 2][..], 99, 1024));
    drop(dir);
    let manifest = read(&*io, "db/MANIFEST");
    assert_eq!(manifest[8..12], 2u32.to_le_bytes());
    assert_eq!(manifest[12..20], 1024u64.to_le_bytes());
    for case in ["flipped size", "missing manifest"] {
        let image = image(&io);
        if case == "missing manifest" {
            // Existing segments block a fresh manifest.
            image.remove(Path::new("db/MANIFEST")).unwrap();
        } else {
            let file = image.open(Path::new("db/MANIFEST")).unwrap();
            image.flip_bit(file, 12).unwrap();
            image.close(file).unwrap();
        }
        let reopened = DataDir::open(image, "db", None, 1024);
        assert!(matches!(reopened, Err(Error::CorruptionDetected { .. })), "{case}");
    }
}

#[test]
fn atomic_install_is_old_or_new_at_every_crash_point() {
    for seed in 0..100 {
        let io = Arc::new(HookIo::sim(seed));
        let _dir = DataDir::open(io.clone(), "db", None, 1024).unwrap();
        atomic_install(io.clone(), Path::new("db"), "TEST", b"old").unwrap();
        let images = io.capture();
        atomic_install(io.clone(), Path::new("db"), "TEST", b"new-content").unwrap();
        assert_eq!(io.open_files(), 0, "seed={seed}");
        for (event, crash) in images.lock().unwrap().iter() {
            let bytes = read(crash, "db/TEST");
            assert!(bytes == b"old" || bytes == b"new-content", "seed={seed}, crash after {event}");
        }
    }
}

#[test]
fn lock_is_first_and_reports_the_holder() {
    let dir = TestDir::new();
    let io = Arc::new(StdIo::new());
    let held = DataDir::open(io.clone(), &dir.0, Some("localhost:77"), 1024).unwrap();
    std::fs::write(dir.0.join("MANIFEST"), b"invalid").unwrap();
    match DataDir::open(io, &dir.0, None, 1024) {
        Err(Error::DataDirLocked { holder }) => {
            assert!(holder.contains(&std::process::id().to_string()), "{holder}");
            assert!(holder.contains("localhost:77"), "{holder}");
        }
        other => panic!("{other:?}"),
    }
    drop(held);
}

#[test]
fn retry_persists_orphan_removal_after_directory_sync_failure() {
    let db = Path::new("db");
    for (case, renamed_spare) in [("allocated orphan", false), ("renamed spare orphan", true)] {
        for seed in 0..32 {
            let io = Arc::new(SimDisk::new(seed));
            let spare = || make_spare(io.clone(), db, 1024, 0, &Mutex::new(())).unwrap();
            let mut dir = DataDir::open(io.clone(), db, None, 1024).unwrap();
            dir.prepare_next_segment(1024).unwrap();
            if renamed_spare {
                spare();
            }
            let id = dir.prepare_next_segment(1024).unwrap();
            assert!(!io.exists(&db.join("spare.zdb")).unwrap(), "{case}: roll took the spare");
            dir.install_manifest(&[1]).unwrap();
            // A crash can also leave a ready spare and a partial fill.
            spare();
            io.close(io.create(&db.join("spare.tmp")).unwrap()).unwrap();
            let leftovers = [dir.segment_path(id), db.join("spare.zdb"), db.join("spare.tmp")];
            drop(dir);
            io.fail_next_sync().unwrap();
            let failed = DataDir::open(io.clone(), db, None, 1024);
            assert!(matches!(failed, Err(Error::SyncFailed(_))), "{case}");
            assert!(leftovers.iter().all(|path| !io.exists(path).unwrap()), "{case}");
            let _retry = DataDir::open(io.clone(), db, None, 1024).unwrap();
            let crash = io.crash().unwrap();
            let gone = leftovers.iter().all(|path| !crash.exists(path).unwrap());
            assert!(gone, "{case}, seed={seed}");
        }
    }
}
