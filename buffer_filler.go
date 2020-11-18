package main

import (
	"crypto/rand"
	"math"
	mathrand "math/rand"
)

/*
	reductionBlockSize	Block size for deduplication and compression
	compressionPercent	Approximate compression percentage for each block compression. Range: [0, 100]. 0 for all zeroes, 100 for uncompressible data
	dedupCortxUnitSize	Blocks are duplicated only within every dedupCortxUnitSize of data. Must be a multiple of reductionBlockSize
	dedupPercent		Approximate percentage of unique blocks within dedupCortxUnitSize. Range: [0, 100]. 0 for dedupCortxUnitSize copies of the same block, 100 for all unique blocks
	fillZerosWithA		When filling buffers with random data according to compressionPercent fill the rest of the buffer with 'A' characters instead of filling with 0s.
 */
func bufferFill(buf []byte, size int64, reductionBlockSize int64, compressionPercent float64, dedupCortxUnitSize int64, dedupPercent float64, fillZerosWithA bool) {
	compression := compressionPercent / 100.
	dedup := dedupPercent / 100.
	// the buf is divided into large blocks, each block size is dedupCortxUnitSize
	// lb for large block
	lb_nr := (size + dedupCortxUnitSize - 1) / dedupCortxUnitSize   // total number of large blocks
	for j := int64(0); j < lb_nr; j++ {
		lb_offset := j * dedupCortxUnitSize                     // large block offset within buf
		lb_length := size - lb_offset                           // remaning size of the large block
		if (lb_length > dedupCortxUnitSize) {
			lb_length = dedupCortxUnitSize
		}
		block_nr := (lb_length + reductionBlockSize - 1) / reductionBlockSize    // number of blocks within the large block
		uniq_block_nr := int64(math.Round(float64(block_nr) * dedup))       // number of unique block within the large block
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

func bufferGenerate(size int64, reductionBlockSize int64, compressionPercent float64, dedupCortxUnitSize int64, dedupPercent float64, fillZerosWithA bool) []byte {
	buf := make([]byte, size, size)
	bufferFill(buf, size, reductionBlockSize, compressionPercent, dedupCortxUnitSize, dedupPercent, fillZerosWithA)
	return buf
}
