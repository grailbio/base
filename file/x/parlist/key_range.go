package parlist

import (
	"errors"
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/grailbio/base/must"
)

// KeyRange is a subset of S3 keyspace. Fields must be valid UTF-8.
// Zero value is entire range.
// Note: S3 has a max key length but we ignore that here and allow keys to be any length.
type KeyRange struct {
	// MinExcl is the lower bound of the range.
	// Note: "" means unbounded below by definition of string comparison.
	// Note: We choose min exclusive because S3's listing API uses an exclusive StartAfter.
	MinExcl string
	// MaxIncl is the upper bound of the range.
	// As a special case, "" means unbounded above.
	MaxIncl string
}

func EmptyKeyRange() KeyRange { return KeyRange{"empty", "empty"} }

// IsEmpty computes if the range is empty by definition (regardless of objects actually in S3).
// Unbounded ranges (like the zero value) are not empty.
func (r KeyRange) IsEmpty() bool {
	if r.MaxIncl == "" {
		return false
	}
	return r.MinExcl >= r.MaxIncl
}

func (r KeyRange) ContainsKey(k string) bool {
	return r.MinExcl < k && (r.MaxIncl == "" || k <= r.MaxIncl)
}

func (r KeyRange) CommonPrefix() string {
	var common int
	lo, hi := r.MinExcl, r.MaxIncl
	for len(lo) > 0 && len(hi) > 0 {
		rl, nl := utf8.DecodeRuneInString(lo)
		must.Truef(rl != utf8.RuneError, "%#v", r)
		rh, nh := utf8.DecodeRuneInString(hi)
		must.Truef(rh != utf8.RuneError, "%#v", r)
		if rl != rh {
			break
		}
		must.Truef(nl == nh, "%#v", r)
		common += nl
		lo, hi = lo[nl:], hi[nh:]
	}
	return r.MinExcl[:common]
}

func (r KeyRange) String() string {
	var (
		common  = r.CommonPrefix()
		minTail = r.MinExcl[len(common):]
		maxTail = r.MaxIncl[len(common):]
	)
	return fmt.Sprintf("%s{%q -> %q}", common, minTail, maxTail)
}

// Halve divides KeyRange in two (in many cases), or gives up and returns ok == false.
// For now, reasonable behavior is implemented if r's bounds are ASCII.
//
// Note: There are some cases where the implementation needs to choose a midpoint character.
// For example:
//   KeyRange{MinExcl: "abc", MaxIncl: "abcz"}.
// We want to divide the space above "abc" in (approximately) half, perhaps at some char ?, like:
//   KeyRange{MinExcl: "abc", MaxIncl: "abc?"}
//   KeyRange{MinExcl: "abc?", MaxExcl: "abcz"}
// Ideally we'd choose ? as the median of the distribution of real key characters, rather than
// assuming we'll only ever see ASCII, or that the distribution is uniform over all of Unicode
// (empirically unrealistic, and thus inefficient, in our real data).
// See assumedMedianASCII.
// TODO: Consider handling ASCII as-is but implementing separate logic for keys containing Unicode.
func (r KeyRange) Halve() (lo, hi KeyRange, _ error) { return r.halve(nonControl) }

// HalveNoSlashes is like Halve but does not introduce any '/'s into the output.
// Input r's bounds must not contain '/' either.
func (r KeyRange) HalveNoSlashes() (lo, hi KeyRange, _ error) { return r.halve(nonControlNorSlash) }

var ErrHalveUnsupported = errors.New("halve is not implemented for this string")

func (r KeyRange) halve(charset asciiSubset) (lo, hi KeyRange, _ error) {
	if r.IsEmpty() {
		return EmptyKeyRange(), EmptyKeyRange(), ErrHalveUnsupported
	}
	var (
		common  = r.CommonPrefix()
		minTail = r.MinExcl[len(common):]
		maxTail = r.MaxIncl[len(common):]
		err     error
	)

	minTail, err = charset.StringToSubset(minTail)
	if err != nil {
		return EmptyKeyRange(), EmptyKeyRange(), err
	}
	maxTail, err = charset.StringToSubset(maxTail)
	if err != nil {
		return EmptyKeyRange(), EmptyKeyRange(), err
	}

	var midTail string
	switch {
	case minTail == "" && maxTail == "":
		// 'Z' is probably closer to the median of real key character distributions than
		// something like unicode.MaxASCII/2. But it's just a guess.
		midTail = string('Z')

	case maxTail == "" && minTail[0] <= unicode.MaxASCII:
		// Bounded lower, unbounded upper. Split the ASCII range; if there are non-ASCII chars,
		// they'll still be included in hi.
		midTail = string((minTail[0] + unicode.MaxASCII + 1) / 2)

	default:
		var ok bool
		midTail, ok = stringMean(minTail, maxTail, charset.MinIncl)
		if !ok {
			return EmptyKeyRange(), EmptyKeyRange(), ErrHalveUnsupported
		}
	}

	midTail, err = charset.StringToASCII(midTail)
	if err != nil {
		return EmptyKeyRange(), EmptyKeyRange(), err
	}

	mid := common + midTail
	return KeyRange{r.MinExcl, mid}, KeyRange{mid, r.MaxIncl}, nil
}

// stringMean computes the approximate arithmetic mean of lo and hi.
// The strings are interpreted as decimals in base B = unicode.MaxASCII, for example:
//   lo = lo[0]/B + lo[1]/B^2 + ...
// This is analogous to interpreting string "314" as 0.314 (but we use byte values of characters,
// so 0 is ASCII 48, etc.).
//
// lo and hi must be valid UTF-8. If the computation reaches a non-ASCII character in the input,
// it aborts and returns "", false.
// TODO(josh): Consider doing something reasonable with Unicode.
//
// Result is computed starting from the left / highest value digits and we approximate by stopping
// just after the strings differ. For example,
//   mean("aaa", "aaazzz🙂")
// is approximately "aaa=" (note that '=' is 'z'/2), without reaching the Unicode character later.
//
// Does not give '/' any special consideration. That is, the computed mean can include '/' that was
// not in the input.
//
// If lo != hi, the strict inequalities lo < mean and mean < hi hold (when ok == true).
func stringMean(lo, hi string, minChar rune) (_ string, ok bool) {
	var base = unicode.MaxASCII + 1 - minChar

	var (
		mean  = make([]rune, 0, max(len(lo), len(hi))+1)
		carry bool

		appendDigitMean = func(a, b rune) {
			v := a + b - 2*minChar
			if carry {
				v += base
			}
			carry = v&1 > 0
			v /= 2
			if v >= base {
				// Carry can overflow this digit. For example, in (0.94 + 0.88)/2 == 0.91,
				// after seeing first digits (9, 8), we don't know if the output is 8 or 9 because
				// it depends on the next ones (4, 8).
				mean[len(mean)-1]++
				v -= base
			}
			mean = append(mean, v+minChar)
		}
	)

	for {
		rl, hasL := popRune(&lo)
		rh, hasH := popRune(&hi)
		if rl > unicode.MaxASCII || rh > unicode.MaxASCII {
			return "", false
		}
		switch {
		case hasL && hasH:
			appendDigitMean(rl, rh)

		case hasL || hasH:
			appendDigitMean(minChar, rl+rh) // Note: One of rl or rh is zero.
			// We're not going to see any more chars from the shorter string, and the mean is
			// already > lo.
			return string(mean), true

		case carry:
			carry = false
			mean = append(mean, minChar+base/2)

		default:
			return string(mean), true
		}
	}
}

// popRune returns the first rune in *s and updates *s to remove it.
// If *s is empty, returns 0, false.
// *s must be valid UTF-8.
func popRune(s *string) (_ rune, nonEmpty bool) {
	if len(*s) == 0 {
		return 0, false
	}
	r, n := utf8.DecodeRuneInString(*s)
	must.Truef(r != utf8.RuneError, "not valid utf-8: %q", *s)
	*s = (*s)[n:]
	return r, true
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
