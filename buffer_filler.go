package main

import (
	"crypto/rand"
	"io/ioutil"
	"math"
	"fmt"
	"os"
	mathrand "math/rand"
)

/*
	reductionBlockSize	Block size for deduplication and compression
	compressionSavings	Approximate savings from compression for each block, %. Range: [0, 100]. 0 for uncompressible data, 100 for all bytes to the same value (0 if fillZerosWithA is false, 'A' otherwise)
	dedupCortxUnitSize	Blocks are duplicated only within every dedupCortxUnitSize of data. Must be a multiple of reductionBlockSize
	dedupSavings		Approximate percentage of non-unique blocks within dedupCortxUnitSize. Range: [0, 100]. 0 for all unique blocks, 100 for dedupCortxUnitSize copies of the same block
	fillZerosWithA		When filling buffers with random data according to compressionSavings fill the rest of the buffer with 'A' characters instead of filling with 0s.
 */
func bufferFill(buf []byte, reductionBlockSize int64, compressionSavings float64, dedupCortxUnitSize int64, dedupSavings float64, fillZerosWithA bool) {
	compression := 1. - compressionSavings / 100.
	dedup := 1. - dedupSavings / 100.
	// the buf is divided into large blocks, each block size is dedupCortxUnitSize
	// lb for large block
	lb_nr := (int64(len(buf)) + dedupCortxUnitSize - 1) / dedupCortxUnitSize  // total number of large blocks
	for j := int64(0); j < lb_nr; j++ {
		lb_offset := j * dedupCortxUnitSize				  // large block offset within buf
		lb_length := int64(len(buf)) - lb_offset                          // remaning size of the large block
		if (lb_length > dedupCortxUnitSize) {
			lb_length = dedupCortxUnitSize
		}
		block_nr := (lb_length + reductionBlockSize - 1) / reductionBlockSize  // number of blocks within the large block
		uniq_block_nr := int64(math.Round(float64(block_nr) * dedup))          // number of unique block within the large block
		if (uniq_block_nr > block_nr) {
			uniq_block_nr = block_nr
		}
		if (uniq_block_nr < 1) {
			uniq_block_nr = 1
		}
		perm := mathrand.Perm(int(block_nr))
		for i, index := range perm {
			block_offset := int64(index) * reductionBlockSize
			block_size := lb_length - block_offset
			if (block_size >  reductionBlockSize) {
				block_size = reductionBlockSize
			}
			block_offset += lb_offset
			block_rand := int64(math.Round(float64(block_size) * compression))
			if (block_rand > block_size) {
				block_rand = block_size
			}
			if (int64(i) < uniq_block_nr) {
				_, err := rand.Read(buf[block_offset : block_offset + block_rand])
				if err != nil {
					panic("Could not fill a buffer with rand.Read()")
				}
				// consider using https://godoc.org/github.com/tmthrgd/go-memset for this
				if fillZerosWithA {
					for i := range buf[block_offset + block_rand : block_offset + block_size] {
						buf[i] = 'A'
					}
				}
			} else {
				src_offset := lb_offset + int64(perm[int64(i) % uniq_block_nr]) * reductionBlockSize
				copy(buf[block_offset : block_offset + block_size],
				     buf[src_offset : src_offset + reductionBlockSize])
			}
		}
	}
}

func bufferGenerate(size int64, reductionBlockSize int64, compressionSavings float64, dedupCortxUnitSize int64, dedupSavings float64, fillZerosWithA bool) []byte {
	buf := make([]byte, size, size)
	bufferFill(buf, reductionBlockSize, compressionSavings, dedupCortxUnitSize, dedupSavings, fillZerosWithA)
	return buf
}

func bufferFillFromFile(buf []byte, filename string) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println("Cannot read %s.", filename)
		os.Exit(1)
	}
	for offset := 0; offset < len(buf); offset += len(content) {
		left := len(buf) - offset
		if left > len(content) {
			left = len(content)
		}
		copy(buf[offset : offset + left], content)
	}
}

func bufferGenerateFromFile(size int64, filename string) []byte {
	buf := make([]byte, size, size)
	bufferFillFromFile(buf, filename)
	return buf
}
