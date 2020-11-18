.PHONY: all build clean interactive test-build test-interactive test

s3bench_binary = s3bench
s3bench_image = s3bench
s3bench_pull_binary_container = s3bench-pull-binary

test_image = s3bench-test

all: build ;

build:
	rm -fv ./$(s3bench_binary)
	docker build -t $(s3bench_image) -f Dockerfile .
	docker create --interactive --tty --name $(s3bench_pull_binary_container) $(s3bench_image) true
	docker cp $(s3bench_pull_binary_container):/s3bench/s3bench ./$(s3bench_binary)
	docker rm -f $(s3bench_pull_binary_container)

clean:
	docker image rm $(s3bench_image) || true

interactive: build
	docker run --interactive --tty --rm $(s3bench_image) || true

test-build:
	docker build -t $(test_image) -f Dockerfile.test .

test-interactive: build test-build
	docker run --interactive --tty --rm $(test_image) /bin/bash || true

test: build test-build
	./s3bench -testReductionFile test-reduction-file -objectSize 8MiB -reductionBlockSize 4KiB -compressionPercent 30 -dedupCortxUnitSize 1MiB -dedupPercent 10
	docker run --tty --rm $(test_image) ./test.py || true
