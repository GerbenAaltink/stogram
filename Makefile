all: build run

build:
	-@rm -r ./stogram
	gcc stogram.c -o stogram -lsqlite3

replication:
	./stogram --port 9000 --db "local3.db" --rhost "127.0.0.1" --rport 8889 --verbose

replication2:
	./stogram --port 9001 --db "local2.db" --rhost "127.0.0.1" --rport 9000 --verbose


run:
	./stogram --verbose --port 8889 --db "local1.db"
