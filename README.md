The grailbio/base project includes all the packages used by many
other grailbio Go packages:

- [recordio](https://godoc.org/github.com/grailbio/base/recordio): encrypted and compressed record oriented files with indexing support
- [file](https://godoc.org/github.com/grailbio/base/file): unified file API for the local file system and S3
- [digest](https://godoc.org/github.com/grailbio/base/digest): common in-memory and serialized representation of digests
- [data](https://godoc.org/github.com/grailbio/base/data): support for measuring and displaying quantities of data
- [intervalmap](https://godoc.org/github.com/grailbio/base/intervalmap): fast interval tree
- [limiter](https://godoc.org/github.com/grailbio/base/limiter): concurrency limiter with context support
- [traverse](https://godoc.org/github.com/grailbio/base/traverse): concurrent and parallel slice traversal
- [state](https://godoc.org/github.com/grailbio/base/state): file-based state management
- [syncqueue](https://godoc.org/github.com/grailbio/base/syncqueue): various flavors of producer-consumer queues
- [unsafe](https://godoc.org/github.com/grailbio/base/unsafe): conversion from []byte to string, etc.
- [compress/libdeflate](https://godoc.org/github.com/grailbio/base/compress/libdeflate): efficient block compression/decompression
- [simd](https://godoc.org/github.com/grailbio/base/simd): fast operations on []byte
- [tsv](https://godoc.org/github.com/grailbio/base/tsv): simple and efficient TSV writer
