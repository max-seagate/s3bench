.PHONY: all

all:
	docker build -t s3bench -f Dockerfile .
	docker run --interactive --tty --rm s3bench || true
