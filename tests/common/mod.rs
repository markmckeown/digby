// Shared helpers for the integration tests.

// Install a logger so error! calls are formatted and written. Output is
// captured by the test harness and only shown for failing tests. Logs at
// error level unless RUST_LOG is set. Safe to call from every test.
pub fn init_logger() {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Error)
        .parse_default_env()
        .try_init();
}
