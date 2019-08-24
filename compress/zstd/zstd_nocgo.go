// +build !cgo

package zstd

import (
	"bytes"
	"io"

	nocgozstd "github.com/klauspost/compress/zstd"
)

func CompressLevel(scratch []byte, in []byte, level int) ([]byte, error) {
	if level < 0 {
		level = 5 // 5 is the default compression const in cgo zstd
	}
	wBuf := bytes.NewBuffer(scratch[:0])
	w, err := nocgozstd.NewWriter(wBuf,
		nocgozstd.WithEncoderLevel(nocgozstd.EncoderLevelFromZstd(level)))
	if err != nil {
		return nil, err
	}
	rBuf := bytes.NewReader(in)
	_, err = io.Copy(w, rBuf)
	if err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return wBuf.Bytes(), nil
}

func Decompress(scratch []byte, in []byte) ([]byte, error) {
	rBuf := bytes.NewReader(in)
	r, err := nocgozstd.NewReader(rBuf)
	if err != nil {
		return nil, err
	}

	wBuf := bytes.NewBuffer(scratch[:0])
	if _, err = io.Copy(wBuf, r); err != nil {
		return nil, err
	}
	r.Close()
	return wBuf.Bytes(), nil
}

type readerWrapper struct {
	*nocgozstd.Decoder
}

func (r *readerWrapper) Close() error {
	r.Decoder.Close()
	return nil
}

func NewReader(r io.Reader) (io.ReadCloser, error) {
	zr, err := nocgozstd.NewReader(r)
	if err != nil {
		return nil, err
	}
	return &readerWrapper{zr}, nil
}

func NewWriter(w io.Writer) (io.WriteCloser, error) {
	return nocgozstd.NewWriter(w)
}
