.PHONY: lint check test clippy fmt fmt-check clippy-fix fix

# Run all CI checks (matches .github/workflows/pr.yml)
lint: check test clippy

# Individual check targets
check:
	cargo check --workspace

test:
	cargo test -p spicebench

clippy:
	cargo clippy -p spicebench --all-targets -- -D warnings

# Format checking
fmt-check:
	cargo fmt --all -- --check

# Fix targets
fmt:
	cargo fmt --all

clippy-fix:
	cargo clippy -p spicebench --all-targets --fix --allow-dirty --allow-staged

# Run all fixes
fix: fmt clippy-fix
