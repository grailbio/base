// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package digest provides a generalized representation for digests
// computed with cryptographic hash functions. It provides an efficient
// in-memory representation as well as serialization.
package digest

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	mathrand "math/rand"
	"strings"
)

const maxSize = 64 // To support SHA-512

// Define digestHash constants to be used during (de)serialization of Digests.
// crypto.Hash values are not guaranteed to be stable over releases.
// The order of digestHashes should never be changed to maintain compatibility
// over time and new values should always be appended. The initial set has been
// ordered to match the order in crypto.Hash at this change to maintain backward
// compatibility.
type digestHash uint

const (
	MD4         digestHash = 1 + iota // crypto.MD4
	MD5                               // crypto.MD5
	SHA1                              // crypto.SHA1
	SHA224                            // crypto.SHA224
	SHA256                            // crypto.SHA256
	SHA384                            // crypto.SHA384
	SHA512                            // crypto.SHA512
	MD5SHA1                           // crypto.MD5SHA1
	RIPEMD160                         // crypto.RIPEMD160
	SHA3_224                          // crypto.SHA3_224
	SHA3_256                          // crypto.SHA3_256
	SHA3_384                          // crypto.SHA3_384
	SHA3_512                          // crypto.SHA3_512
	SHA512_224                        // crypto.SHA512_224
	SHA512_256                        // crypto.SHA512_256
	BLAKE2s_256                       // crypto.BLAKE2s_256
	BLAKE2b_256                       // crypto.BLAKE2b_256
	BLAKE2b_384                       // crypto.BLAKE2b_384
	BLAKE2b_512                       // crypto.BLAKE2b_512

	zeroString = "<zero>"
)

var (
	digestToCryptoHashes = map[digestHash]crypto.Hash{
		MD4:         crypto.MD4,
		MD5:         crypto.MD5,
		SHA1:        crypto.SHA1,
		SHA224:      crypto.SHA224,
		SHA256:      crypto.SHA256,
		SHA384:      crypto.SHA384,
		SHA512:      crypto.SHA512,
		MD5SHA1:     crypto.MD5SHA1,
		RIPEMD160:   crypto.RIPEMD160,
		SHA3_224:    crypto.SHA3_224,
		SHA3_256:    crypto.SHA3_256,
		SHA3_384:    crypto.SHA3_384,
		SHA3_512:    crypto.SHA3_512,
		SHA512_224:  crypto.SHA512_224,
		SHA512_256:  crypto.SHA512_256,
		BLAKE2s_256: crypto.BLAKE2s_256,
		BLAKE2b_256: crypto.BLAKE2b_256,
		BLAKE2b_384: crypto.BLAKE2b_384,
		BLAKE2b_512: crypto.BLAKE2b_512,
	}
	cryptoToDigestHashes = map[crypto.Hash]digestHash{} // populated by init()
)

var (
	shortSuffix = [maxSize - 4]byte{}
	zeros       = [maxSize]byte{}
)

var (
	name = map[crypto.Hash]string{
		crypto.MD4:        "md4",
		crypto.MD5:        "md5",
		crypto.SHA1:       "sha1",
		crypto.SHA224:     "sha224",
		crypto.SHA256:     "sha256",
		crypto.SHA384:     "sha384",
		crypto.SHA512:     "sha512",
		crypto.SHA512_224: "sha512_224",
		crypto.SHA512_256: "sha512_256",
		crypto.SHA3_224:   "sha3_224",
		crypto.SHA3_256:   "sha3_256",
		crypto.SHA3_384:   "sha3_384",
		crypto.SHA3_512:   "sha3_512",
		crypto.MD5SHA1:    "md5sha1",
		crypto.RIPEMD160:  "ripemd160",
	}
	hashes = map[string]crypto.Hash{} // populated by init()
)

var (
	// An attempt was made to parse an invalid digest
	ErrInvalidDigest = errors.New("invalid digest")
	// A Digest's hash function was not imported.
	ErrHashUnavailable = errors.New("the requested hash function is not available")
	// The Digest's hash did not match the hash of the Digester.
	ErrWrongHash = errors.New("wrong hash")
	// An EOF was encountered while attempting to read a Digest.
	ErrShortRead = errors.New("short read")
)

func init() {
	for h, name := range name {
		hashes[name] = h
	}
	for dh, ch := range digestToCryptoHashes {
		cryptoToDigestHashes[ch] = dh
	}
}

// Digest represents a digest computed with a cryptographic hash
// function. It uses a fixed-size representation and is directly
// comparable.
type Digest struct {
	h crypto.Hash
	b [maxSize]byte
}

var _ gob.GobEncoder = Digest{}
var _ gob.GobDecoder = (*Digest)(nil)

// GobEncode implements Gob encoding for digests.
func (d Digest) GobEncode() ([]byte, error) {
	b := make([]byte, binary.MaxVarintLen64+d.h.Size())
	n := binary.PutUvarint(b, uint64(d.h))
	copy(b[n:], d.b[:d.h.Size()])
	return b[:n+d.h.Size()], nil
}

// GobDecode implements Gob decoding for digests.
func (d *Digest) GobDecode(p []byte) error {
	h, n := binary.Uvarint(p)
	if n == 0 {
		return errors.New("short buffer")
	}
	if n < 0 {
		return errors.New("invalid hash")
	}
	d.h = crypto.Hash(h)
	if len(p)-n != d.h.Size() {
		return errors.New("invalid digest")
	}
	copy(d.b[:], p[n:])
	return nil
}

// Parse parses a string representation of Digest, as defined by
// Digest.String().
func Parse(s string) (Digest, error) {
	if s == "" || s == zeroString {
		return Digest{}, nil
	}
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return Digest{}, ErrInvalidDigest
	}
	name, hex := parts[0], parts[1]
	h, ok := hashes[name]
	if !ok {
		return Digest{}, ErrInvalidDigest
	}
	return ParseHash(h, hex)
}

// ParseHash parses hex string hx produced by the the hash h into a
// Digest.
func ParseHash(h crypto.Hash, hx string) (Digest, error) {
	if !h.Available() {
		return Digest{}, ErrHashUnavailable
	}
	b, err := hex.DecodeString(hx)
	if err != nil {
		return Digest{}, err
	}
	d := Digest{h: h}
	copy(d.b[:], b)
	if !d.valid() {
		return Digest{}, ErrInvalidDigest
	}
	return d, nil
}

// New returns a new literal digest with the provided hash and
// value.
func New(h crypto.Hash, b []byte) Digest {
	d := Digest{h: h}
	copy(d.b[:], b)
	return d
}

// IsZero returns whether the digest is the zero digest.
func (d Digest) IsZero() bool { return d.h == 0 }

// Hash returns the cryptographic hash used to produce this Digest.
func (d Digest) Hash() crypto.Hash { return d.h }

// Hex returns the padded hexadecimal representation of the Digest.
func (d Digest) Hex() string {
	n := d.h.Size()
	return fmt.Sprintf("%0*x", 2*n, d.b[:n])
}

// HexN returns the padded hexadecimal representation of the digest's
// first n bytes. N must be smaller or equal to the digest's size, or
// else it panics.
func (d Digest) HexN(n int) string {
	if d.h.Size() < n {
		panic("n is too large")
	}
	return fmt.Sprintf("%0*x", 2*n, d.b[:n])
}

// Short returns a short (prefix) version of the Digest's hexadecimal
// representation.
func (d Digest) Short() string {
	return d.Hex()[0:8]
}

// Name returns the name of the digest's hash.
func (d Digest) Name() string {
	return name[d.h]
}

// Less defines an order of digests of the same hash. panics if two
// Less digests with different hashes are compared.
func (d Digest) Less(e Digest) bool {
	if d.h != e.h {
		panic("incompatible hashes")
	}
	return bytes.Compare(d.b[:], e.b[:]) < 0
}

// Bytes returns the byte representation for this digest.
func (d Digest) Bytes() []byte {
	var b bytes.Buffer
	if _, err := WriteDigest(&b, d); err != nil {
		panic("failed to write file digest " + d.String() + ": " + err.Error())
	}
	return b.Bytes()
}

// Mix mixes digests d and e with XOR.
func (d *Digest) Mix(e Digest) {
	if d.h == 0 {
		*d = e
		return
	}
	if d.h != e.h {
		panic("mismatched hashes")
	}
	for i := range d.b {
		d.b[i] ^= e.b[i]
	}
}

// Truncate truncates digest d to n bytes. Truncate
// panics if n is greater than the digest's hash size.
func (d *Digest) Truncate(n int) {
	if d.h.Size() < n {
		panic("n is too large")
	}
	copy(d.b[n:], zeros[:])
}

// IsShort tells whether d is a "short" digest, comprising
// only the initial 4 bytes.
func (d Digest) IsShort() bool {
	return bytes.HasSuffix(d.b[:], shortSuffix[:])
}

// IsAbbrev tells whether d is an "abbreviated" digest, comprising
// no more than half of the digest bytes.
func (d Digest) IsAbbrev() bool {
	return bytes.HasSuffix(d.b[:], zeros[d.h.Size()/2:])
}

// NPrefix returns the number of nonzero leading bytes in the
// digest, after which the remaining bytes are zero.
func (d Digest) NPrefix() int {
	for i := d.h.Size() - 1; i >= 0; i-- {
		if d.b[i] != 0 {
			return i + 1
		}
	}
	return 0
}

// Expands tells whether digest d expands digest e.
func (d Digest) Expands(e Digest) bool {
	n := e.NPrefix()
	return bytes.HasPrefix(d.b[:], e.b[:n])
}

// String returns the full string representation of the digest: the digest
// name, followed by ":", followed by its hexadecimal value.
func (d Digest) String() string {
	if d.IsZero() {
		return zeroString
	}
	return fmt.Sprintf("%s:%s", name[d.h], d.Hex())
}

// ShortString returns a short representation of the digest, comprising
// the digest name and its first n bytes.
func (d Digest) ShortString(n int) string {
	if d.IsZero() {
		return zeroString
	}
	return fmt.Sprintf("%s:%s", name[d.h], d.HexN(n))
}

func (d Digest) valid() bool {
	return d.h.Available() && len(d.b) >= d.h.Size()
}

// MarshalJSON marshals the Digest into JSON format.
func (d Digest) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON unmarshals a digest from JSON data.
func (d *Digest) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	var err error
	*d, err = Parse(s)
	return err
}

// Digester computes digests based on a cryptographic hash function.
type Digester crypto.Hash

// New returns a new digest with the provided literal contents. New
// panics if the digest size does not match the hash function's length.
func (d Digester) New(b []byte) Digest {
	if crypto.Hash(d).Size() != len(b) {
		panic("digest: bad digest length")
	}
	return New(crypto.Hash(d), b)
}

// Parse parses a string into a Digest with the cryptographic hash of
// Digester. The input string is in the form of Digest.String, except
// that the hash name may be omitted--it is then instead assumed to
// be the hash function associated with the Digester.
func (d Digester) Parse(s string) (Digest, error) {
	if s == "" || s == zeroString {
		return Digest{h: crypto.Hash(d)}, nil
	}
	parts := strings.Split(s, ":")
	switch len(parts) {
	default:
		return Digest{}, ErrInvalidDigest
	case 1:
		return ParseHash(crypto.Hash(d), s)
	case 2:
		dgst, err := Parse(s)
		if err != nil {
			return Digest{}, err
		}
		if dgst.h != crypto.Hash(d) {
			return Digest{}, ErrWrongHash
		}
		return dgst, nil
	}
}

// FromBytes computes a Digest from a slice of bytes.
func (d Digester) FromBytes(p []byte) Digest {
	w := crypto.Hash(d).New()
	if _, err := w.Write(p); err != nil {
		panic("hash returned error " + err.Error())
	}
	return New(crypto.Hash(d), w.Sum(nil))
}

// FromString computes a Digest from a string.
func (d Digester) FromString(s string) Digest {
	return d.FromBytes([]byte(s))
}

// FromDigests computes a Digest over other Digests.
func (d Digester) FromDigests(digests ...Digest) Digest {
	w := crypto.Hash(d).New()
	for _, d := range digests {
		// TODO(saito,pknudsgaaard,schandra)
		//
		// grail.com/pipeline/release/internal/reference passes an empty Digest and
		// fails here. We need to be more principled about the values passed here,
		// so we intentionally drop errors here.
		WriteDigest(w, d)
	}
	return New(crypto.Hash(d), w.Sum(nil))
}

// Rand returns a random digest generated by the random
// provided generator. If no generator is provided (r is nil),
// Rand uses the system's cryptographically secure random
// number generator.
func (d Digester) Rand(r *mathrand.Rand) Digest {
	dg := Digest{h: crypto.Hash(d)}
	var (
		err error
		p   = dg.b[:dg.h.Size()]
	)
	if r != nil {
		_, err = r.Read(p)
	} else {
		_, err = rand.Read(p)
	}
	if err != nil {
		panic(err)
	}
	return dg
}

// NewWriter returns a Writer that can be used to compute
// Digests of long inputs.
func (d Digester) NewWriter() Writer {
	return Writer{crypto.Hash(d), crypto.Hash(d).New()}
}

// Writer provides an io.Writer to which digested bytes are
// written and from which a Digest is produced.
type Writer struct {
	h crypto.Hash
	w hash.Hash
}

func (d Writer) Write(p []byte) (n int, err error) {
	return d.w.Write(p)
}

// Digest produces the current Digest of the Writer.
// It does not reset its internal state.
func (d Writer) Digest() Digest {
	return New(d.h, d.w.Sum(nil))
}

// WriteDigest is a convenience function to write a (binary)
// Digest to an io.Writer. Its format is two bytes representing
// the hash function, followed by the hash value itself.
//
// Writing a zero digest is disallowed; WriteDigest panics in
// this case.
func WriteDigest(w io.Writer, d Digest) (n int, err error) {
	if d.IsZero() {
		panic("digest.WriteDigest: attempted to write a zero digest")
	}
	digestHash, ok := cryptoToDigestHashes[d.h]
	if !ok {
		return n, fmt.Errorf("cannot convert %v to a digestHash", d.h)
	}
	b := [2]byte{byte(digestHash >> 8), byte(digestHash & 0xff)}
	n, err = w.Write(b[:])
	if err != nil {
		return n, err
	}
	m, err := w.Write(d.b[:d.h.Size()])
	return n + m, err
}

// ReadDigest is a convenience function to read a (binary)
// Digest from an io.Reader, as written by WriteDigest.
func ReadDigest(r io.Reader) (Digest, error) {
	var d Digest
	n, err := r.Read(d.b[0:2])
	if err != nil {
		return Digest{}, err
	}
	if n < 2 {
		return Digest{}, ErrShortRead
	}
	d.h = digestToCryptoHashes[digestHash(d.b[0])<<8|digestHash(d.b[1])]
	if !d.h.Available() {
		return Digest{}, ErrHashUnavailable
	}
	n, err = r.Read(d.b[0:d.h.Size()])
	if err != nil {
		return Digest{}, err
	}
	if n < d.h.Size() {
		return Digest{}, ErrShortRead
	}
	return d, nil
}

// MarshalJSON generates a JSON format byte slice from a Digester.
func (d Digester) MarshalJSON() ([]byte, error) {
	txt, ok := name[crypto.Hash(d)]

	if !ok {
		return nil, fmt.Errorf("Cannot convert %v to string", d)
	}

	return []byte(fmt.Sprintf(`"%s"`, txt)), nil
}

// UnmarshalJSON converts from a JSON format byte slice to a Digester.
func (d *Digester) UnmarshalJSON(b []byte) error {
	str := string(b)

	val, ok := hashes[strings.Trim(str, `"`)]

	if !ok {
		return fmt.Errorf("Cannot convert %s to digest.Digester", string(b))
	}

	*d = Digester(val)
	return nil
}
