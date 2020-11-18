package main

import (
	"crypto/rand"
	"math"
	mathrand "math/rand"
)

func bufferFill(buf []byte, size int64, reductionBlockSize int64, compressionPercent float64, dedupCortxUnitSize int64, dedupPercent float64) {
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
			} else {
				src_offset := lb_offset + int64(perm[int64(i) % uniq_block_nr]) * reductionBlockSize
				copy(buf[block_offset : block_offset + block_size],
				     buf[src_offset : src_offset + reductionBlockSize])
			}
		}
	}
}

func bufferGenerate(size int64, reductionBlockSize int64, compressionPercent float64, dedupCortxUnitSize int64, dedupPercent float64) []byte {
	buf := make([]byte, size, size)
	bufferFill(buf, size, reductionBlockSize, compressionPercent, dedupCortxUnitSize, dedupPercent)
	return buf
}
