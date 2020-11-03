all: clean run

clean:
	rm -f /Users/shivam/tmp/*log

run:
	RUST_BACKTRACE=1 RUSTFLAGS=-Awarnings cargo build
	target/debug/cs380p-2pc -S 0.99 -s 0.99 -c 4 -p 10 -r 10 -m $(M) -v 0
