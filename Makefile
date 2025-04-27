test:
	RUST_BACKTRACE=1 RUSTFLAGS="--cfg mloom" LOOM_CHECKPOINT_FILE="my_test.json" LOOM_CHECKPOINT_INTERVAL=1 LOOM_LOG=trace LOOM_LOCATION=1 cargo test storage::test::test_disab --release -- --nocapture

.PHONY: test
