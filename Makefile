

all: clean run


clean:
	rm -f /Users/shivam/tmp/*log

build:
    RUST_BACKTRACE=1 RUSTFLAGS=-Awarnings cargo build

run: build
	RUST_BACKTRACE=1 RUSTFLAGS=-Awarnings cargo build
	target/debug/cs380p-2pc -s 0.75 -c 1 -p 10 -r 50 -m $(M) -v 3
