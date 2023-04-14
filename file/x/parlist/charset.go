package parlist

import "fmt"

// asciiSubset defines a character encoding for Unicode characters minus some subset of ASCII.
// For non-ASCII characters, the encoding equals UTF-8 (no customization).
// For ASCII characters outside of the subset, encoding is undefined (returns 0 for error).
// For ASCII characters in the subset, the encoded values are contiguous within 0 < x <= 127
// (Though the ASCII encodings for the subset characters are not necessarily contiguous. That is,
// the subset encoding may reorder some characters).
type asciiSubset struct {
	// Name is a readable name for error messages.
	Name string
	// MinIncl is the smallest encoding value in the subset.
	MinIncl rune
	// ToSubset converts ASCII encoding to this asciiSubset's encoding, returning 0 for characters
	// outside of the subset. ToASCII is the inverse.
	ToSubset, ToASCII func(rune) rune
}

// nonControl is an encoding for the non-control ASCII characters.
var nonControl = asciiSubset{
	Name:    "ascii non-control",
	MinIncl: 32,
	ToSubset: func(r rune) rune {
		if r >= 32 {
			return r
		}
		return 0
	},
	ToASCII: func(r rune) rune {
		if r >= 32 {
			return r
		}
		return 0
	},
}

// nonControlNorSlash is an encoding for the non-control ASCII characters minus '/'.
var nonControlNorSlash = asciiSubset{
	Name:    "ascii non-control non-slash",
	MinIncl: 33,
	ToSubset: func(r rune) rune {
		switch {
		case r > '/':
			return r
		case 32 <= r && r < '/':
			return r + 1
		}
		return 0
	},
	ToASCII: func(r rune) rune {
		switch {
		case r > '/':
			return r
		case 32 < r && r <= '/':
			return r - 1
		}
		return 0
	},
}

func (a asciiSubset) StringToSubset(s string) (string, error) {
	return a.transform(s, a.ToSubset, fmt.Errorf("could not convert ASCII->'%s': %q", a.Name, s))
}
func (a asciiSubset) StringToASCII(s string) (string, error) {
	return a.transform(s, a.ToASCII, fmt.Errorf("could not convert '%s'->ASCII: %q", a.Name, s))
}

func (a asciiSubset) transform(s string, f func(rune) rune, err error) (string, error) {
	newS := make([]rune, 0, len(s)) // Note: Not a good size estimate, but it's fine.
	for _, r := range s {
		newR := f(r)
		if newR == 0 {
			return "", err
		}
		newS = append(newS, newR)
	}
	return string(newS), nil
}
