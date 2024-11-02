
all: env build

env:
	-@rm -r ./venv
	python3 -m venv ./venv

bench:
	./venv/bin/stogram_bench 1000

debug:
	./venv/bin/stogram_debug 

db:
	./venv/bin/stogram_db

run:
	./venv/bin/stogram

build:
	
	. ./venv/bin/activate && cd stogram_client && make build
