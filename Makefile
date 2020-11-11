all: clean run

clean:
	rm -f /Users/shivam/tmp/*log

build:
	RUST_BACKTRACE=1 RUSTFLAGS=-Awarnings cargo run

run:
	RUST_BACKTRACE=1 RUSTFLAGS=-Awarnings cargo build
	target/debug/cs380p-2pc  -S 0.75 -s 1 -c 50 -p 20 -r 150 -m $(M) -v 0