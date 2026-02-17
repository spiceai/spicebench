lint:
	cargo check --workspace
	cargo clippy -p spicebench --all-targets -- -D warnings
	cargo test -p spicebench

test:
	cargo test -p spicebench
