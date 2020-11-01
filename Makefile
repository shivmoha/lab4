

all: run

run:
	RUST_BACKTRACE=1 RUSTFLAGS=-Awarnings cargo run