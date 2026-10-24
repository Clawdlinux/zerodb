#![cfg(feature = "sim")]
#![allow(clippy::unwrap_used)]

#[test]
// Dev dependencies also enable sim. Debug schedules stay opt-in with --ignored.
#[cfg_attr(debug_assertions, ignore = "slow in debug")]
fn seeded_crash_recovery() {
    let var = |name| std::env::var(name).ok().map(|s| s.parse::<u64>().unwrap());
    let first = var("ZERODB_SIM_SEED");
    let count = var("ZERODB_SIM_SEEDS").unwrap_or(if first.is_some() { 1 } else { 500 });
    let first = first.unwrap_or(0);
    assert!(count > 0, "ZERODB_SIM_SEEDS must be positive");
    for seed in (0..count).map(|offset| first.wrapping_add(offset)) {
        if let Err(error) = std::panic::catch_unwind(|| zerodb_tape::sim::run_seed(seed)) {
            eprintln!(
                "simulation seed={seed}\nrepro: ZERODB_SIM_SEED={seed} cargo test -p zerodb-tape --release --features sim --test sim"
            );
            std::panic::resume_unwind(error);
        }
    }
    eprintln!("simulation passed: {count} seeds starting at {first}");
}
