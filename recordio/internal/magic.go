package internal

import (
	"hash/crc32"
)

// NumMagicBytes is the size of a magic header stored at the beginning of every
// chunk.
const NumMagicBytes = 8

// MagicBytes is stored in the first 8 bytes of any chunk.
type MagicBytes = [NumMagicBytes]byte

// MagicLegacy is the legacy "unpacked" block header.
var MagicLegacyUnpacked = MagicBytes{0xfc, 0xae, 0x95, 0x31, 0xf0, 0xd9, 0xbd, 0x20}

// MagicPacked is the legacy "packed" block header, and the v2 data block header.
var MagicPacked = MagicBytes{0x2e, 0x76, 0x47, 0xeb, 0x34, 0x07, 0x3c, 0x2e}

// MagicHeader is a constant random 8 bytes used to distinguish the header block
// of a v2 recordio file.
var MagicHeader = MagicBytes{0xd9, 0xe1, 0xd9, 0x5c, 0xc2, 0x16, 0x04, 0xf7}

// MagicTrailer is a constant random 8 bytes used to distinguish the trailer block
// of a v2 recordio file.
var MagicTrailer = MagicBytes{0xfe, 0xba, 0x1a, 0xd7, 0xcb, 0xdf, 0x75, 0x3a}

// MagicInvalid is a sentinel. It is never stored in storage.
var MagicInvalid = MagicBytes{0xe4, 0xe7, 0x9a, 0xc1, 0xb3, 0xf6, 0xb7, 0xa2}

// MaxReadRecordSize defines a max size for a record when reading to avoid
// crashes for unreasonable requests.
var MaxReadRecordSize = uint64(1 << 29)

// IEEECRC is used to compute IEEE CRC chunk checksums.
var IEEECRC *crc32.Table

func init() {
	IEEECRC = crc32.MakeTable(crc32.IEEE)
}
