.PHONY: build test fmt lint install

build:
	cargo build --release

test:
	cargo test --all-targets

fmt:
	cargo fmt --all

lint:
	cargo clippy --all-targets -- -D warnings

install:
	cargo install --path . --locked

