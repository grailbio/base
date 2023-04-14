package parlist

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/grailbio/base/must"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyRangeHalve(t *testing.T) {
	type testCase struct {
		in         KeyRange
		wantMid    string
		lineNumber int
	}
	var (
		testsCommon = []testCase{
			{KeyRange{"", ""}, "Z", lineno()},
			{KeyRange{"100", "300"}, "200", lineno()},
			{KeyRange{"\x7D", ""}, "\x7E", lineno()},
			// Note that 0x5F is above 0x3F, as desired.
			{KeyRange{"8\x3F", "9"}, "8\x5F", lineno()},
			// Note: 0x22 is averaged with 0x20, the minimum char.
			{KeyRange{"", "\x22ĉ"}, "\x21", lineno()},
			{KeyRange{"a", "c\x40ĉ"}, "b\x30", lineno()},
		}
		// Some cases are rounded differently (since / and no-/ have different charsets).
		testsHalve = []testCase{
			// Note: Carry from (1+2)/2 is added to '0'.
			{KeyRange{"100", "200"}, "1`0", lineno()},
		}
		testsHalveNoSlashes = []testCase{
			// Note: Carry from (1+2)/2 propagated to '0', and then another carry, etc.
			{KeyRange{"100", "200"}, "1__P", lineno()},
			// Regression test for a bug during development.
			{KeyRange{"570734", "570`nv?"}, "570L U0", lineno()},
		}
		testsAll []testCase
	)
	testsAll = append(testsAll, testsCommon...)
	testsAll = append(testsAll, testsHalve...)
	testsAll = append(testsAll, testsHalveNoSlashes...)
	for _, test := range testsAll {
		// Double-check that the test is valid.
		require.Less(t, test.in.MinExcl, test.wantMid, "%v", test)
		if test.in.MaxIncl != "" {
			require.Less(t, test.wantMid, test.in.MaxIncl, "%v", test)
		}
	}
	t.Run("halve", func(t *testing.T) {
		for _, test := range append(testsHalve, testsCommon...) {
			t.Run(fmt.Sprintf("line %d", test.lineNumber), func(t *testing.T) {

				gotLo, gotHi, gotErr := test.in.Halve()
				require.NoError(t, gotErr)
				assert.Equal(t, KeyRange{test.in.MinExcl, test.wantMid}, gotLo)
				assert.Equal(t, KeyRange{test.wantMid, test.in.MaxIncl}, gotHi)
			})
		}
	})
	t.Run("halveNoSlashes", func(t *testing.T) {
		for _, test := range append(testsHalveNoSlashes, testsCommon...) {
			t.Run(fmt.Sprintf("line %d", test.lineNumber), func(t *testing.T) {

				gotLo, gotHi, gotErr := test.in.HalveNoSlashes()
				require.NoError(t, gotErr)
				assert.Equal(t, KeyRange{test.in.MinExcl, test.wantMid}, gotLo)
				assert.Equal(t, KeyRange{test.wantMid, test.in.MaxIncl}, gotHi)
			})
		}
	})
}

// TODO: Randomized tests that marshal ASCII into a big decimal and compare result to real math.
func TestKeyRangeHalveSkipped(t *testing.T) {
	tests := []KeyRange{
		EmptyKeyRange(),
		KeyRange{"", "ābc"},
		KeyRange{"ābc", ""},
		KeyRange{"ā", "ĉ"},
	}
	for testIdx, test := range tests {
		t.Run("slash/"+fmt.Sprint(testIdx), func(t *testing.T) {
			_, _, gotErr := test.Halve()
			assert.Equal(t, ErrHalveUnsupported, gotErr)
		})
		t.Run("no_slash/"+fmt.Sprint(testIdx), func(t *testing.T) {
			_, _, gotErr := test.HalveNoSlashes()
			assert.Equal(t, ErrHalveUnsupported, gotErr)
		})
	}
}

func lineno() int {
	_, _, line, ok := runtime.Caller(1)
	must.True(ok)
	return line
}
