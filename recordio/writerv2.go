// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package recordio

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/recordio/internal"
)

const (
	// DefaultFlushParallelism is the default value for WriterOpts.MaxFlushParallelism.
	DefaultFlushParallelism = uint32(8)

	// MaxFlushParallelism is the max allowed value for WriterOpts.MaxFlushParallelism.
	MaxFlushParallelism = uint32(128)

	// MaxPackedItems defines the max items that can be
	// packed into a single record by a PackedWriter.
	MaxPackedItems = uint32(10 * 1024 * 1024)
	// DefaultPackedItems defines the default number of items that can
	// be packed into a single record by a PackedWriter.
	DefaultPackedItems = uint32(16 * 1024)
)

// ItemLocation identifies the location of an item in a recordio file.
type ItemLocation struct {
	// Location of the first byte of the block within the file. Unit is bytes.
	Block uint64
	// Index of the item within the block. The Nth item in the block (N=1,2,...)
	// has value N-1.
	Item int
}

// IndexFunc runs after an item is flushed to storage.  Parameter "loc" is the
// location of the item in the file.  It can be later passed to Reader.Seek
// method to seek to the item.
type IndexFunc func(loc ItemLocation, item interface{}) error

// WriterOpts defines options used when creating a new writer.
type WriterOpts struct {
	// Marshal is called for every item added by Append. It serializes the the
	// record. If Marshal is nil, it defaults to a function that casts the value
	// to []byte and returns it. Marshal may be called concurrently.
	Marshal MarshalFunc

	// Index is called for every item added, just before it is written to
	// storage. Index callback may be called concurrently and out of order of
	// locations.
	//
	// After Index is called, the Writer guarantees that it never touches
	// the value again. The application may recycle the value in a freepool, if it
	// desires. Index may be nil.
	Index IndexFunc

	// Transformer specifies a list of functions to compress, encrypt, or modify
	// data in any other way, just before a block is written to storage.
	//
	// Each entry in Transformer must be of form "name" or "name config.."  The
	// "name" is matched against the registry (see RegisterTransformer).  The
	// "config" part is passed to the transformer factory function.  If "name" is
	// not registered, the writer will fail immediately.
	//
	// If Transformers contains multiple strings, Transformers[0] is invoked
	// first, then its results are passed to Transformers[1], so on.
	//
	// If len(Transformers)==0, then an identity transformer is used. It will
	// return the block as is.
	//
	// Recordio package includes the following standard transformers:
	//
	//  "zstd N" (N is -1 or an integer from 0 to 22): zstd compression level N.
	//  If " N" part is omitted or N=-1, the default compression level is used.
	//  To use zstd, import the 'recordiozstd' package and call
	//  'recordiozstd.Init()' in an init() function.
	//
	//  "flate N" (N is -1 or an integer from 0 to 9): flate compression level N.
	//  If " N" part is omitted or N=-1, the default compression level is used.
	//  To use flate, import the 'recordioflate' package and call
	//  'recordioflate.Init()' in an init() function.
	Transformers []string

	// MaxItems is the maximum number of items to pack into a single record.
	// It defaults to DefaultPackedItems if set to 0.
	// If MaxItems exceeds MaxPackedItems it will silently set to MaxPackedItems.
	MaxItems uint32

	// MaxFlushParallelism limits the maximum number of block flush operations in
	// flight before blocking the application. It defaults to
	// DefaultMaxFlushParallelism.
	MaxFlushParallelism uint32

	// TODO(saito) Consider providing a flag to allow out-of-order writes, like
	// ConcurrentPackedWriter.
}

// Writer defines an interface for recordio writer. An implementation must be
// thread safe.
//
// Legal path expression is defined below. Err can be called at any time, so it
// is not included in the expression. ? means 0 or 1 call, * means 0 or more
// calls.
//
//   AddHeader*
//   (Append|Flush)*
//   SetTrailer?
//   Finish
type Writer interface {
	// Add an arbitrary metadata to the file. This method must be called
	// before any other Append* or Set* functions. If the key had been already added
	// to the header, this method will overwrite it with the value.
	//
	// REQUIRES: Append, SetTrailer, Finish have not been called.
	AddHeader(key string, value interface{})

	// Write one item. The marshaler will be eventually called to
	// serialize the item.  The type of v must match the input type for
	// the Marshal function passed when the writer is created. Note that
	// since marhsalling is performed asynchronously, the object passed
	// to append should be considered owned by the writer, and must not
	// be reused by the caller.
	//
	// The writer flushes items to the storage in the order of addition.
	//
	// REQUIRES: Finish and SetTrailer have not been called.
	Append(v interface{})

	// Schedule to flush the current block. The next item will be written in a new
	// block. This method just schedules for flush, and returns before the block
	// is actually written to storage. Call Wait to wait for Flush to finish.
	Flush()

	// Block the caller until all the prior Flush calls finish.
	Wait()

	// Add an arbitrary data at the end of the file. After this function, no
	// {Add*,Append*,Set*} functions may be called.
	//
	// REQUIRES: AddHeader(KeyTrailer, true) has been called.
	SetTrailer([]byte)

	// Err returns any error encountered by the writer. Once Err() becomes
	// non-nil, it stays so.
	Err() error

	// Finish must be called at the end of writing. Finish will internally call
	// Flush, then returns the value of Err. No method, other than Err, shall be
	// called in a future.
	Finish() error
}

type blockType int

const (
	bTypeInvalid blockType = iota
	bTypeHeader
	bTypeBody
	bTypeTrailer
)

var magicv2Bytes = []internal.MagicBytes{
	internal.MagicInvalid,
	internal.MagicHeader,
	internal.MagicPacked,
	internal.MagicTrailer,
}

// Contents of one recordio block.
type writerv2Block struct {
	bType blockType

	// Objects added by Append.
	objects []interface{}
	rawData []byte

	// Result of serializing objects.  {bufs,objects} are used iff btype = body
	serialized []byte

	// Block write order.  The domain is (0,1,2,...)
	flushSeq int

	// Tmp used during data serialization
	tmpBuf [][]byte
}

func (b *writerv2Block) reset() {
	b.serialized = b.serialized[:0]
	b.objects = b.objects[:0]
	b.bType = bTypeInvalid
}

// State of the writerv2. The state transitions in one direction only.
type writerState int

const (
	// No writes started. AddHeader() can be done in this state only.
	wStateInitial writerState = iota
	// The main state. Append and Flush can be called.
	wStateWritingBody
	// State after a SetTrailer call.
	wStateWritingTrailer
	// State after Finish call.
	wStateFinished
)

// Implementation of Writer
type writerv2 struct {
	// List of empty writerv2Blocks. Capacity is fixed at
	// opts.MaxFlushParallelism.
	freeBlocks chan *writerv2Block
	opts       WriterOpts
	err        errors.Once
	fq         flushQueue

	mu           sync.Mutex
	state        writerState
	header       ParsedHeader
	curBodyBlock *writerv2Block
}

// For serializing block writes. Thread safe.
type flushQueue struct {
	freeBlocks chan *writerv2Block   // Copy of writerv2.freeBlocks.
	opts       WriterOpts            // Copy of writerv2.opts.
	err        *errors.Once          // Copy of writerv2.err.
	wr         *internal.ChunkWriter // Raw chunk writer.

	transform TransformFunc

	mu sync.Mutex
	// flushing is true iff. flushBlocks() is scheduled.
	flushing bool
	// block sequence numbers are dense integer sequence (0, 1, 2, ...)  assigned
	// to blocks. Blocks are written to the storage in the sequence order.
	nextSeq int                    // Seq# to be assigned to the next block.
	lastSeq int                    // Seq# of last block flushed to storage.
	queue   map[int]*writerv2Block // Blocks ready to be flushed. Keys are seq#s.
}

// Assign a new block-flush sequence number.
func (fq *flushQueue) newSeq() int {
	fq.mu.Lock()
	seq := fq.nextSeq
	fq.nextSeq++
	fq.mu.Unlock()
	return seq
}

func idMarshal(scratch []byte, v interface{}) ([]byte, error) {
	return v.([]byte), nil
}

// NewWriter creates a new writer.  New users should use this class instead of
// Writer, PackedWriter, or ConcurrentPackedWriter.
//
// Caution: files created by this writer cannot be read by a legacy
// recordio.Scanner.
func NewWriter(wr io.Writer, opts WriterOpts) Writer {
	if opts.Marshal == nil {
		opts.Marshal = idMarshal
	}
	if opts.MaxItems == 0 {
		opts.MaxItems = DefaultPackedItems
	}
	if opts.MaxItems > MaxPackedItems {
		opts.MaxItems = MaxPackedItems
	}
	if opts.MaxFlushParallelism == 0 {
		opts.MaxFlushParallelism = DefaultFlushParallelism
	}
	if opts.MaxFlushParallelism > MaxFlushParallelism {
		opts.MaxFlushParallelism = MaxFlushParallelism
	}

	w := &writerv2{
		opts:       opts,
		freeBlocks: make(chan *writerv2Block, opts.MaxFlushParallelism),
	}
	w.fq = flushQueue{
		wr:         internal.NewChunkWriter(wr, &w.err),
		opts:       opts,
		freeBlocks: w.freeBlocks,
		err:        &w.err,
		lastSeq:    -1,
		queue:      make(map[int]*writerv2Block),
	}
	for i := uint32(0); i < opts.MaxFlushParallelism; i++ {
		w.freeBlocks <- &writerv2Block{
			objects: make([]interface{}, 0, opts.MaxItems+1),
		}
	}
	var err error
	if w.fq.transform, err = registry.getTransformer(opts.Transformers); err != nil {
		w.err.Set(err)
	}
	for _, val := range opts.Transformers {
		w.header = append(w.header, KeyValue{KeyTransformer, val})
	}
	return w
}

func (w *writerv2) AddHeader(key string, value interface{}) {
	w.mu.Lock()
	if w.state != wStateInitial {
		panic(fmt.Sprintf("AddHeader: wrong state: %v", w.state))
	}
	w.header = append(w.header, KeyValue{key, value})
	w.mu.Unlock()
}

func (w *writerv2) startFlushHeader() {
	data, err := w.header.marshal()
	if err != nil {
		w.err.Set(err)
		return
	}
	b := <-w.freeBlocks
	b.bType = bTypeHeader
	b.rawData = data
	b.flushSeq = w.fq.newSeq()
	go w.fq.serializeAndEnqueueBlock(b)
}

func (w *writerv2) startFlushBodyBlock() {
	b := w.curBodyBlock
	w.curBodyBlock = nil
	b.bType = bTypeBody
	b.flushSeq = w.fq.newSeq()
	go w.fq.serializeAndEnqueueBlock(b)
}

func (w *writerv2) Append(v interface{}) {
	w.mu.Lock()
	if w.state == wStateInitial {
		w.startFlushHeader()
		w.state = wStateWritingBody
	} else if w.state != wStateWritingBody {
		panic(fmt.Sprintf("Append: wrong state: %v", w.state))
	}
	if w.curBodyBlock == nil {
		w.curBodyBlock = <-w.freeBlocks
	}
	w.curBodyBlock.objects = append(w.curBodyBlock.objects, v)
	if len(w.curBodyBlock.objects) >= cap(w.curBodyBlock.objects) {
		w.startFlushBodyBlock()
	}
	w.mu.Unlock()
}

func (w *writerv2) Flush() {
	w.mu.Lock()
	if w.state == wStateInitial {
		w.mu.Unlock()
		return
	}
	if w.state != wStateWritingBody {
		panic(fmt.Sprintf("Flush: wrong state: %v", w.state))
	}
	if w.curBodyBlock != nil {
		w.startFlushBodyBlock()
	}
	w.mu.Unlock()
}

func generatePackedHeaderv2(items [][]byte) []byte {
	// 1 varint for # items, n for the size of each of n items.
	hdrSize := (len(items) + 1) * binary.MaxVarintLen32
	hdr := make([]byte, hdrSize)

	// Write the number of items in this record.
	pos := binary.PutUvarint(hdr, uint64(len(items)))
	// Write the size of each item.
	for _, p := range items {
		pos += binary.PutUvarint(hdr[pos:], uint64(len(p)))
	}
	hdr = hdr[:pos]
	return hdr
}

// Produce a packed recordio block.
func (fq *flushQueue) serializeBlock(b *writerv2Block) {
	getChunks := func(n int) [][]byte {
		if cap(b.tmpBuf) >= n+1 {
			b.tmpBuf = b.tmpBuf[:n+1]
		} else {
			b.tmpBuf = make([][]byte, n+1)
		}
		return b.tmpBuf
	}
	if fq.err.Err() != nil {
		return
	}
	var tmpBuf [][]byte // tmpBuf[0] is for the packed header.
	if b.bType == bTypeBody {
		tmpBuf = getChunks(len(b.objects))
		// Marshal items into bytes.
		for i, v := range b.objects {
			s, err := fq.opts.Marshal(tmpBuf[i+1], v)
			if err != nil {
				fq.err.Set(err)
			}
			tmpBuf[i+1] = s
		}
	} else {
		tmpBuf = getChunks(1)
		tmpBuf[1] = b.rawData
	}

	tmpBuf[0] = generatePackedHeaderv2(tmpBuf[1:])
	transform := idTransform
	if b.bType == bTypeBody || b.bType == bTypeTrailer {
		transform = fq.transform
	}

	var err error
	if b.serialized, err = transform(b.serialized, tmpBuf); err != nil {
		fq.err.Set(err)
	}
}

// Schedule "b" for writes. Caller must have marshaled and transformed "b"
// before the call.  It's ok to call enqueue concurrently; blocks are written to
// the storage in flushSeq order.
func (fq *flushQueue) enqueueBlock(b *writerv2Block) {
	fq.mu.Lock()
	fq.queue[b.flushSeq] = b
	if !fq.flushing && b.flushSeq == fq.lastSeq+1 {
		fq.flushing = true
		fq.mu.Unlock()
		fq.flushBlocks()
	} else {
		fq.mu.Unlock()
	}
}

func (fq *flushQueue) serializeAndEnqueueBlock(b *writerv2Block) {
	fq.serializeBlock(b)
	fq.enqueueBlock(b)
}

func (fq *flushQueue) flushBlocks() {
	fq.mu.Lock()
	if !fq.flushing {
		panic(fq)
	}

	for {
		b, ok := fq.queue[fq.lastSeq+1]
		if !ok {
			break
		}
		delete(fq.queue, b.flushSeq)
		fq.lastSeq++
		fq.mu.Unlock()

		fq.flushBlock(b)
		b.reset()
		fq.freeBlocks <- b
		fq.mu.Lock()
	}
	if !fq.flushing {
		panic(fq)
	}
	fq.flushing = false
	fq.mu.Unlock()
}

func (fq *flushQueue) flushBlock(b *writerv2Block) {
	offset := uint64(fq.wr.Len())
	if fq.err.Err() == nil {
		fq.wr.Write(magicv2Bytes[b.bType], b.serialized)
	}
	if b.bType == bTypeBody && fq.opts.Index != nil {
		// Call the indexing funcs.
		//
		// TODO(saito) Run this code in a separate thread.
		ifn := fq.opts.Index
		for i := range b.objects {
			loc := ItemLocation{Block: offset, Item: i}
			if err := ifn(loc, b.objects[i]); err != nil {
				fq.err.Set(err)
			}
		}
	}
}

func (w *writerv2) SetTrailer(data []byte) {
	w.mu.Lock()
	if !w.header.HasTrailer() {
		panic(fmt.Sprintf("settrailer: Key '%v' must be set to true", KeyTrailer))
	}
	if w.state == wStateInitial {
		w.startFlushHeader()
	} else if w.state == wStateWritingBody {
		if w.curBodyBlock != nil {
			w.startFlushBodyBlock()
		}
	} else {
		panic(fmt.Sprintf("SetTrailer: wrong state: %v", w.state))
	}
	if w.curBodyBlock != nil {
		panic(w)
	}
	w.state = wStateWritingTrailer
	w.mu.Unlock()

	b := <-w.freeBlocks
	b.bType = bTypeTrailer
	b.rawData = make([]byte, len(data))
	copy(b.rawData, data)
	b.flushSeq = w.fq.newSeq()
	go w.fq.serializeAndEnqueueBlock(b)
}

func (w *writerv2) Err() error {
	return w.err.Err()
}

func (w *writerv2) Wait() {
	w.mu.Lock()
	n := 0
	if w.curBodyBlock != nil {
		n++
	}

	tmp := make([]*writerv2Block, 0, cap(w.freeBlocks))
	for n < cap(w.freeBlocks) {
		b := <-w.freeBlocks
		tmp = append(tmp, b)
		n++
	}

	for _, b := range tmp {
		w.freeBlocks <- b
	}
	w.mu.Unlock()
}

func (w *writerv2) Finish() error {
	if w.state == wStateInitial {
		w.startFlushHeader()
		w.state = wStateWritingBody
	}
	if w.state == wStateWritingBody {
		if w.curBodyBlock != nil {
			w.startFlushBodyBlock()
		}
	} else if w.state != wStateWritingTrailer {
		panic(w)
	}
	if w.curBodyBlock != nil {
		w.startFlushBodyBlock()
	}
	w.state = wStateFinished
	// Drain all ongoing flushes.
	for i := 0; i < cap(w.freeBlocks); i++ {
		<-w.freeBlocks
	}
	close(w.freeBlocks)
	if len(w.fq.queue) > 0 {
		panic(w)
	}
	return w.err.Err()
}
