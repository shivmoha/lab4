all: clean run

clean:
	rm -rf ./tmp/*log
	rm -rf ./tmp/coordinator.log

build:
	RUST_BACKTRACE=1 RUSTFLAGS=-Awarnings cargo run

run:
	RUST_BACKTRACE=1 RUSTFLAGS=-Awarnings cargo build
	target/debug/cs380p-2pc  -S 0.95 -s 0.99 -f 0.99 -c 10 -p 5 -r 50 -m $(M) -v 3 -l ./tmp