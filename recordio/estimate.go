package recordio

import (
	"encoding/binary"

	"github.com/grailbio/base/recordio/internal"
)

// RequiredSpaceUpperBound returns an upper bound on the space required to
// store n items of itemSizes for a specified record size.
func RequiredSpaceUpperBound(itemSizes []int64, recordSize int64) int64 {

	// Max number of chunks required per record.
	// reqChunksPerRecord is Ceil(recordSize / internal.MaxChunkPayloadSize)
	reqChunksPerRecord := recordSize / internal.MaxChunkPayloadSize
	if (recordSize % internal.MaxChunkPayloadSize) != 0 {
		reqChunksPerRecord++
	}

	// Max payload = UpperBound(header) + payload.
	// 1 varint for # items, n for the size of each of n items.
	// Using binary.MaxVarintLen64 since we want an upper bound.
	hdrSizeUBound := (len(itemSizes) + 1) * binary.MaxVarintLen64
	maxPayload := int64(hdrSizeUBound)
	for _, s := range itemSizes {
		maxPayload += s
	}

	// Max number of records required for payload.
	// reqRecordsForPayload is Ceil(maxPayload / recordSize)
	reqRecordsForPayload := maxPayload / recordSize
	if (maxPayload % recordSize) != 0 {
		reqRecordsForPayload++
	}

	// Max number of chunks required = chunks for payload + 2 chunks for header and trailer.
	reqChunksForPayload := (reqChunksPerRecord * reqRecordsForPayload) + int64(2)

	// Upper bound on the space required.
	return reqChunksForPayload * internal.ChunkSize
}
