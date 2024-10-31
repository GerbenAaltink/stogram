all: build run

build:
	-@rm -r ./stogram
	gcc stogram.c -o stogram -lsqlite3

run:
	./stogram