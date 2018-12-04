// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"

	"github.com/grailbio/base/errors"
)

type chunkFlag uint32

const (
	chunkHeaderSize     = 28
	chunkSize           = 32 << 10
	maxChunkPayloadSize = chunkSize - chunkHeaderSize
)

// Chunk layout:
//
//   magic [8B]
//   crc   [4B LE]
//   flag  [4B LE]
//   size  [4B LE]
//   total [4B LE]
//   index [4B LE]
//   data [size]
//   padding [32768 - 28 - size]
//
// magic: one of MagicHeader, MagicPacked, MagicTrailer.
// size: size of the chunk payload (data). size <=  (32<<10) - 24
// padding: garbage data added to make the chunk size exactly 32768B.
//
// total: the total # of chunks in the blocks.
// index: the index of the chunk within the block.  Index is 0 for the first
// block, 1 for the 2nd block, and so on.
// flag: unused now.
//
// crc: IEEE CRC32 of of the succeeding fields: size, index, flag, and data.
//  Note: padding is not included in the CRC.
type chunkHeader [chunkHeaderSize]byte

func (h *chunkHeader) TotalChunks() int {
	return int(binary.LittleEndian.Uint32(h[20:]))
}

func (h *chunkHeader) Index() int {
	return int(binary.LittleEndian.Uint32(h[24:]))
}

var chunkPadding [maxChunkPayloadSize]byte

// Seek to "off". Returns nil iff the seek ptr moves to "off".
func Seek(r io.ReadSeeker, off int64) error {
	n, err := r.Seek(off, io.SeekStart)
	if err != nil {
		return err
	}
	if n != off {
		return fmt.Errorf("seek: got %v, expect %v", n, off)
	}
	return nil
}

func init() {
	temp := [4]byte{0xde, 0xad, 0xbe, 0xef}
	for i := range chunkPadding {
		chunkPadding[i] = temp[i%len(temp)]
	}
}

// ChunkWriter implements low-level block-write operations. It takes logical
// block and stores it as a sequence of chunks. Thread compatible.
type ChunkWriter struct {
	nWritten int64
	w        io.Writer
	err      *errors.Once
	crc      hash.Hash32
}

// Len returns the number of bytes successfully written so far.
// The value is meaningful only when err.Err()==nil.
func (w *ChunkWriter) Len() int64 {
	return w.nWritten
}

// Write one block. An error is reported through w.err.
func (w *ChunkWriter) Write(magic MagicBytes, payload []byte) {
	var header chunkHeader
	copy(header[:], magic[:])

	chunkIndex := 0
	totalChunks := (len(payload)-1)/maxChunkPayloadSize + 1
	for {
		var chunkPayload []byte
		lastChunk := false
		if len(payload) <= maxChunkPayloadSize {
			lastChunk = true
			chunkPayload = payload
			payload = nil
		} else {
			chunkPayload = payload[:maxChunkPayloadSize]
			payload = payload[maxChunkPayloadSize:]
		}
		binary.LittleEndian.PutUint32(header[12:], uint32(0))
		binary.LittleEndian.PutUint32(header[16:], uint32(len(chunkPayload)))
		binary.LittleEndian.PutUint32(header[20:], uint32(totalChunks))
		binary.LittleEndian.PutUint32(header[24:], uint32(chunkIndex))

		w.crc.Reset()
		w.crc.Write(header[12:])
		w.crc.Write(chunkPayload)
		csum := w.crc.Sum32()
		binary.LittleEndian.PutUint32(header[8:], csum)
		w.doWrite(header[:])
		w.doWrite(chunkPayload)
		chunkIndex++
		if lastChunk {
			paddingSize := maxChunkPayloadSize - len(chunkPayload)
			if paddingSize > 0 {
				w.doWrite(chunkPadding[:paddingSize])
			}
			break
		}
	}
	if chunkIndex != totalChunks {
		panic(fmt.Sprintf("nchunks %d, total %d", chunkIndex, totalChunks))
	}
}

func (w *ChunkWriter) doWrite(data []byte) {
	n, err := w.w.Write(data)
	if err != nil {
		w.err.Set(err)
		return
	}
	w.nWritten += int64(len(data))
	if n != len(data) {
		w.err.Set(fmt.Errorf("Failed to write %d bytes (got %d)", len(data), n))
	}
}

// NewChunkWriter creates a new chunk writer. Any error is reported through
// "err".
func NewChunkWriter(w io.Writer, err *errors.Once) *ChunkWriter {
	return &ChunkWriter{w: w, err: err, crc: crc32.New(IEEECRC)}
}

// ChunkScanner reads a sequence of chunks and reconstructs a logical
// block. Thread compatible.
type ChunkScanner struct {
	r   io.ReadSeeker
	err *errors.Once

	fileSize int64
	off      int64
	limit    int64

	magic  MagicBytes
	chunks [][]byte

	pool                 [][]byte
	unused               int // the first unused buf in pool.
	approxChunksPerBlock float64
}

// NewChunkScanner creates a new chunk scanner. Any error is reported through "err".
func NewChunkScanner(r io.ReadSeeker, err *errors.Once) *ChunkScanner {
	rx := &ChunkScanner{r: r, err: err}
	// Compute the file size.
	var e error
	if rx.fileSize, e = r.Seek(0, io.SeekEnd); e != nil {
		rx.err.Set(e)
	}
	rx.err.Set(Seek(r, 0))
	rx.limit = math.MaxInt64
	return rx
}

// LimitShard limits this scanner to scan the blocks belonging to a shard range
// [start,limit) out of [0, nshard). The shard range begins at the scanner's
// current offset, which must be on a block boundary. The file (beginning at the
// current scanner offset) is divided into n shards. Each shard scans blocks
// until the next segment. If a shard begins in the middle of a block, that
// block belongs to the previous shard.
func (r *ChunkScanner) LimitShard(start, limit, nshard int) {
	// Compute the offset and limit for shard-of-nshard.
	// Invariant: limit is the offset at or after which a new block
	// should not be scanned.
	numChunks := (r.fileSize - r.off) / chunkSize
	chunksPerShard := float64(numChunks) / float64(nshard)
	startOff := r.off
	r.off = startOff + int64(float64(start)*chunksPerShard)*chunkSize
	r.limit = startOff + int64(float64(limit)*chunksPerShard)*chunkSize
	if start == 0 {
		// No more work to do. We assume LimitShard is called on a block boundary.
		return
	}
	r.err.Set(Seek(r.r, r.off))
	if r.err.Err() != nil {
		return
	}
	var header chunkHeader
	if !r.readChunkHeader(&header) {
		return
	}
	if r.err.Err() != nil {
		return
	}
	index := header.Index()
	if index == 0 {
		// No more work to do: we're already on a block boundary.
		return
	}
	// We're in the middle of a block. The current block belongs to the
	// previous shard, so we forward to the next block boundary.
	total := header.TotalChunks()
	if total <= index {
		r.err.Set(errors.New("invalid chunk header"))
		return
	}
	r.off += chunkSize * int64(total-index)
	r.err.Set(Seek(r.r, r.off))
}

// Tell returns the file offset of the next block to be read.
// Any error is reported in r.Err()
func (r *ChunkScanner) Tell() int64 {
	return r.off
}

// Seek moves the read pointer so that next Scan() will move to the block at the
// given file offset. Any error is reported in r.Err()
func (r *ChunkScanner) Seek(off int64) {
	r.off = off
	r.err.Set(Seek(r.r, off))
}

// Scan reads the next block. It returns false on EOF or any error.
// AnB error is reported in r.Err()
func (r *ChunkScanner) Scan() bool {
	r.resetChunks()
	r.magic = MagicInvalid
	if r.err.Err() != nil {
		return false
	}
	if r.off >= r.limit {
		r.err.Set(io.EOF)
		return false
	}
	totalChunks := -1
	for {
		chunkMagic, _, nchunks, index, chunkPayload := r.readChunk()
		if chunkMagic == MagicInvalid || r.err.Err() != nil {
			return false
		}
		if len(r.chunks) == 0 {
			r.magic = chunkMagic
			totalChunks = nchunks
		}
		if chunkMagic != r.magic {
			r.err.Set(fmt.Errorf("Magic number changed in the middle of a chunk sequence, got %v, expect %v",
				r.magic, chunkMagic))
			return false
		}
		if len(r.chunks) != index {
			r.err.Set(fmt.Errorf("Chunk index mismatch, got %v, expect %v for magic %x",
				index, len(r.chunks), r.magic))
			return false
		}
		if nchunks != totalChunks {
			r.err.Set(fmt.Errorf("Chunk nchunk mismatch, got %v, expect %v for magic %x",
				nchunks, totalChunks, r.magic))
			return false
		}
		r.chunks = append(r.chunks, chunkPayload)
		if index == totalChunks-1 {
			break
		}
	}
	return true
}

// Block returns the current block contents.
//
// REQUIRES: Last Scan() call returned true.
func (r *ChunkScanner) Block() (MagicBytes, [][]byte) {
	return r.magic, r.chunks
}

func (r *ChunkScanner) readChunkHeader(header *chunkHeader) bool {
	_, err := io.ReadFull(r.r, header[:])
	if err != nil {
		r.err.Set(err)
		return false
	}
	r.off, err = r.r.Seek(-chunkHeaderSize, io.SeekCurrent)
	r.err.Set(err)
	return true
}

// Read one chunk. On Error or EOF, returns MagicInvalid. The caller should
// check r.err.Err() to distinguish EOF and a real error.
func (r *ChunkScanner) readChunk() (MagicBytes, chunkFlag, int, int, []byte) {
	chunkBuf := r.allocChunk()
	n, err := io.ReadFull(r.r, chunkBuf)
	r.off += int64(n)
	if err != nil {
		r.err.Set(err)
		return MagicInvalid, chunkFlag(0), 0, 0, nil
	}
	header := chunkBuf[:chunkHeaderSize]

	var magic MagicBytes
	copy(magic[:], header[:])
	expectedCsum := binary.LittleEndian.Uint32(header[8:])
	flag := chunkFlag(binary.LittleEndian.Uint32(header[12:]))
	size := binary.LittleEndian.Uint32(header[16:])
	totalChunks := int(binary.LittleEndian.Uint32(header[20:]))
	index := int(binary.LittleEndian.Uint32(header[24:]))
	if size > maxChunkPayloadSize {
		r.err.Set(fmt.Errorf("Invalid chunk size %d", size))
		return MagicInvalid, chunkFlag(0), 0, 0, nil
	}

	chunkPayload := chunkBuf[chunkHeaderSize : chunkHeaderSize+size]
	actualCsum := crc32.Checksum(chunkBuf[12:chunkHeaderSize+size], IEEECRC)
	if expectedCsum != actualCsum {
		r.err.Set(fmt.Errorf("Chunk checksum mismatch, expect %d, got %d",
			actualCsum, expectedCsum))
	}
	return magic, flag, totalChunks, index, chunkPayload
}

func (r *ChunkScanner) resetChunks() {
	// Avoid keeping too much data in the freepool.  If the pool size exceeds 2x
	// the avg size of recent blocks, trim it down.
	nChunks := float64(len(r.chunks))
	if r.approxChunksPerBlock == 0 {
		r.approxChunksPerBlock = nChunks
	} else {
		r.approxChunksPerBlock =
			r.approxChunksPerBlock*0.9 + nChunks*0.1
	}
	max := int(r.approxChunksPerBlock*2) + 1
	if len(r.pool) > max {
		r.pool = r.pool[:max]
	}
	r.unused = 0
	r.chunks = r.chunks[:0]
}

func (r *ChunkScanner) allocChunk() []byte {
	for len(r.pool) <= r.unused {
		r.pool = append(r.pool, make([]byte, chunkSize))
	}
	b := r.pool[r.unused]
	r.unused++
	if len(b) != chunkSize {
		panic(r)
	}
	return b
}

// ReadLastBlock reads the trailer. Sets err if the trailer does not exist, or
// is corrupt. After the call, the read pointer is at an undefined position so
// the user must call Seek() explicitly.
func (r *ChunkScanner) ReadLastBlock() (MagicBytes, [][]byte) {
	var err error
	r.off, err = r.r.Seek(-chunkSize, io.SeekEnd)
	if err != nil {
		r.err.Set(err)
		return MagicInvalid, nil
	}
	magic, _, totalChunks, index, payload := r.readChunk()
	if magic != MagicTrailer {
		r.err.Set(fmt.Errorf("Missing magic trailer; found %v", magic))
		return MagicInvalid, nil
	}
	if index == 0 && totalChunks == 1 {
		// Fast path for a single-chunk trailer.
		return magic, [][]byte{payload}
	}
	// Seek to the beginning of the block.
	r.off, err = r.r.Seek(-int64(index+1)*chunkSize, io.SeekEnd)
	if err != nil {
		r.err.Set(err)
		return MagicInvalid, nil
	}
	if !r.Scan() {
		r.err.Set(fmt.Errorf("Failed to read trailer"))
		return MagicInvalid, nil
	}
	return r.magic, r.chunks
}
