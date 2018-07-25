package recordio

import (
	"github.com/grailbio/base/recordio/internal"
)

// TransformFunc is called to (un)compress or (un)encrypt data. Parameter
// scratch is passed as an performance hint. If the result of the transformation
// fits in scratch, the function should store the result in scratch and return
// it as the first return value. Else, it should allocate a new []byte and
// return it.
type TransformFunc func(scratch []byte, in [][]byte) (out []byte, err error)

// FormatVersion defines the file-format version. Not for general use.  It may
// be removed without notice.
type FormatVersion int

const (
	// V1 is pre 2018-02 format
	V1 FormatVersion = 1
	// V2 is post 2018-02 format
	V2 FormatVersion = 2
)

// MaxReadRecordSize defines a max size for a record when reading to avoid
// crashes for unreasonable requests.
var MaxReadRecordSize = internal.MaxReadRecordSize

// MarshalFunc is called to serialize data.  Parameter scratch is passed as an
// performance hint. If the result of the transformation fits in scratch, the
// function should store the result in scratch and return it as the first return
// value. Else, it should allocate a new []byte and return it.
type MarshalFunc func(scratch []byte, v interface{}) ([]byte, error)

// MagicPacked is the chunk header for legacy and v2 data chunks. Not for
// general use.
var MagicPacked = internal.MagicPacked
