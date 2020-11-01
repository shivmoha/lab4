

all: run


build:
    RUST_BACKTRACE=1 RUSTFLAGS=-Awarnings cargo build

run: build
	RUST_BACKTRACE=1 RUSTFLAGS=-Awarnings cargo build
	target/debug/cs380p-2pc -s 1.0 -c 1 -p 2 -r 4 -m $(M) -v 3
