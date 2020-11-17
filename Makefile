.PHONY: all build clean interactive test

s3bench_binary = s3bench
s3bench_image = s3bench
s3bench_pull_binary_container = s3bench-pull-binary

all: build ;

build:
	docker build -t $(s3bench_image) -f Dockerfile .
	rm -fv ./$(s3bench_binary)
	docker create --interactive --tty --name $(s3bench_pull_binary_container) $(s3bench_image) true
	docker cp $(s3bench_pull_binary_container):/s3bench/s3bench ./$(s3bench_binary)
	docker rm -f $(s3bench_pull_binary_container)

clean:
	docker image rm $(s3bench_image) || true

interactive: build
	docker run --interactive --tty --rm $(s3bench_image) || true

test: build
	./s3bench -testReductionFile test -objectSize 8Mb -reductionBlockSize 4096 -compressionRatioPercent 30 -dedupCortxUnitSize 1Mb -dedupRatioPercent 10
