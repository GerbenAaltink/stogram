
all: env build

env:
	-@rm -r ./venv
	python3 -m venv ./venv

bench:
	python bench.py 100


run:
	./venv/bin/stogram

build:
	
	. ./venv/bin/activate && cd stogram_client && make build
