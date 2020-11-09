all: clean run

clean:
	rm -f /Users/shivam/tmp/*log

build:
	RUST_BACKTRACE=1 RUSTFLAGS=-Awarnings cargo run

run:
	RUST_BACKTRACE=1 RUSTFLAGS=-Awarnings cargo build
	target/debug/cs380p-2pc -S 0.01 -s 0.99 -c 2 -p 1 -r 2 -m $(M) -v 3