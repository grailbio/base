package stringintern

import (
	"reflect"
)

// Intern will recursively traverse one or more objects and collapse all strings that are identical to the same pointer, saving memory.
// Inputs must be pointer types.
// String map keys are not interned.
// The path to all fields must be exported.  It is not possible to modify unexported fields in a safe way.
// Example usage:
//     var x = ... some complicated type with strings
//     stringintern.Intern(&x)
// Warning:  This is a potentially dangerous operation.
//		Extreme care must be taken that no pointers exist to other structures that should not be modified.
//		This method is not thread safe.  No other threads should be reading or writing to x while it is being interned.
// It is safest to use this code for testing purposes to see how much memory can be saved by interning but then do the interning explicitly:
// 	sizeBefore := memsize.DeepSize(&x)
// 	stringintern.Intern(&x)
// 	sizeAfter := memsize.DeepSize(&x)
func Intern(x ...interface{}) {
	myinterner := interner{
		dict:   make(map[string]string),
		locMap: make(map[addressAndType]struct{}),
	}
	for _, val := range x {
		value := reflect.ValueOf(val)
		if value.Kind() != reflect.Ptr {
			panic("input kind must be a pointer")
		}
		myinterner.intern(value)
	}
}

type interner struct {
	// dict stores the mapping of strings to their canonical interned version.
	dict map[string]string
	// keeps track of which memory locations have already been scanned.
	// it is necessary to also store type because structs and fields can have the same address and must be differentiated.
	locMap map[addressAndType]struct{}
}

type addressAndType struct {
	address uintptr
	tp      reflect.Type
}

func (s *interner) intern(x reflect.Value) {
	if x.CanAddr() {
		addr := x.UnsafeAddr()
		x.Type().Name()
		if _, alreadyProcessed := s.locMap[addressAndType{addr, x.Type()}]; alreadyProcessed {
			return
		}
		s.locMap[addressAndType{addr, x.Type()}] = struct{}{} // mark current memory location
	}
	switch x.Kind() {
	case reflect.String:
		if x.CanSet() {
			val := x.String()
			s.internString(&val)
			x.SetString(val)
		}
	case reflect.Float64, reflect.Float32,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Complex64, reflect.Complex128,
		reflect.Invalid, reflect.Chan, reflect.Bool, reflect.Uintptr, reflect.Func:
		// noop.  don't do anything.
	case reflect.Struct:
		for i := 0; i < x.NumField(); i++ {
			s.intern(x.Field(i))
		}
	case reflect.Ptr, reflect.Interface:
		if !x.IsNil() {
			s.intern(x.Elem())
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < x.Len(); i++ {
			s.intern(x.Index(i))
		}
	case reflect.Map:
		for _, key := range x.MapKeys() {
			val := x.MapIndex(key)
			if val.Kind() == reflect.String {
				stringVal := val.String()
				s.internString(&stringVal)
				x.SetMapIndex(key, reflect.ValueOf(stringVal))
			} else {
				s.intern(val)
			}
		}
	}
}

// takes a pointer to a string.  If string has previously been seen, it will change to interned version.
// otherwise adds to dictionary of interned strings.
func (s *interner) internString(x *string) {
	if val, ok := s.dict[*x]; ok {
		*x = val
	} else {
		s.dict[*x] = *x
	}
}
