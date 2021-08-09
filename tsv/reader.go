package tsv

import (
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	"github.com/grailbio/base/errors"
)

type columnFormat struct {
	fieldName  string       // Go struct field name.
	fieldIdx   []int        // Go struct field index.
	columnName string       // expected column name in TSV. Defaults to fieldName unless `tsv:"colname"` tag is set.
	kind       reflect.Kind // type of the column.
	fmt        string       // Optional format directive for writing this value.
	index      int          // index of this column in a row, 0-based.
	offset     uintptr      // byte offset of this field within the Go struct.
}

type rowFormat []columnFormat

// Reader reads a TSV file. It wraps around the standard csv.Reader and allows
// parsing row contents into a Go struct directly. Thread compatible.
//
// TODO(saito) Support passing a custom bool parser.
//
// TODO(saito) Support a custom "NA" detector.
type Reader struct {
	*csv.Reader

	// HasHeaderRow should be set to true to indicate that the input contains a
	// single header row that lists column names of the rows that follow.  It must
	// be set before reading any data.
	HasHeaderRow bool

	// UseHeaderNames causes the reader to set struct fields by matching column
	// names to struct field names (or `tsv` tag). It must be set before reading
	// any data.
	//
	// If not set, struct fields are filled in order, EVEN IF HasHeaderRow=true.
	// If set, all struct fields must have a corresponding column in the file.
	// An error will be reported through Read().
	//
	// REQUIRES: HasHeaderRow=true
	UseHeaderNames bool

	// RequireParseAllColumns causes Read() report an error if there are columns
	// not listed in the passed-in struct. It must be set before reading any data.
	//
	// REQUIRES: HasHeaderRow=true
	RequireParseAllColumns bool

	nRow int // # of rows read so far, excluding the header.

	// columnIndex x maps colname -> colindex (0-based). Filled from the header
	// line.
	columnIndex map[string]int

	cachedRowType   reflect.Type
	cachedRowFormat rowFormat
}

// NewReader creates a new TSV reader that reads from the given input.
func NewReader(in io.Reader) *Reader {
	r := &Reader{
		Reader: csv.NewReader(in),
	}
	r.Reader.Comma = '\t'
	r.ReuseRecord = true
	return r
}

func (r *Reader) validateRowFormat(format rowFormat) error {
	if r.RequireParseAllColumns && len(format) != len(r.columnIndex) {
		return fmt.Errorf("extra columns found in %+v does not match format %v", r.columnIndex, format)
	}
	for i := range format {
		col := &format[i]
		var ok bool
		if col.index, ok = r.columnIndex[col.columnName]; !ok {
			return fmt.Errorf("column %s does not appear in the header: %+v", col.columnName, r.columnIndex)
		}
	}
	sort.Slice(format, func(i, j int) bool {
		return format[i].index < format[j].index
	})
	return nil
}

func parseRowFormat(typ reflect.Type) (rowFormat, error) {
	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("destination must be a pointer to struct, but found %v", typ)
	}
	typ = typ.Elem()
	nField := typ.NumField()
	var format rowFormat
	for i := 0; i < nField; i++ {
		f := typ.Field(i)
		if f.PkgPath != "" { // Unexported field?
			if tag := f.Tag.Get("tsv"); tag != "" {
				return nil, fmt.Errorf("unexported field '%s' should not have a tsv tag '%s'", f.Name, tag)
			}
			continue
		}
		columnName := f.Name
		var fmt string
		if tag := f.Tag.Get("tsv"); tag != "" {
			if tag == "-" {
				continue
			}
			tagArray := strings.Split(tag, ",")
			if tagArray[0] != "" {
				columnName = tagArray[0]
			}
			for _, tag := range tagArray[1:] {
				if strings.HasPrefix(tag, "fmt=") {
					fmt = tag[4:]
				}
			}
		}
		format = append(format, columnFormat{
			fieldName:  f.Name,
			fieldIdx:   f.Index,
			columnName: columnName,
			kind:       f.Type.Kind(),
			fmt:        fmt,
			index:      len(format),
			offset:     f.Offset,
		})
	}
	return format, nil
}

func (r *Reader) wrapError(err error, col columnFormat) error {
	var name string
	if col.columnName != col.fieldName {
		name = fmt.Sprintf("'%s' (Go field '%s')", col.columnName, col.fieldName)
	} else {
		name = fmt.Sprintf("'%s'", col.columnName)
	}
	return errors.E(err, fmt.Sprintf("line %d, column %d, %s", r.nRow, col.index, name))
}

// fillRow fills Go struct fields from the TSV row.  dest is the pointer to the
// struct, and format defines the struct format.
func (r *Reader) fillRow(val interface{}, row []string) error {
	typ := reflect.TypeOf(val).Elem()
	p := unsafe.Pointer(reflect.ValueOf(val).Pointer())
	if r.RequireParseAllColumns && len(r.cachedRowFormat) != len(row) { // check this for headerless TSVs
		return fmt.Errorf("extra columns found in %+v", r.cachedRowFormat)
	}

	for _, col := range r.cachedRowFormat {
		if len(row) < col.index {
			return r.wrapError(fmt.Errorf("row has only %d columns", len(row)), col)
		}
		colVal := row[col.index]
		if col.fmt != "" {
			// Not all format directives are recognized while scanning. Try to
			// standardize some of the common options.
			colfmt := col.fmt
			if strings.ContainsAny(colfmt, "efg") {
				// Standardize all base 10 floating point number formats to 'g', and
				// drop precision and width which are not supported while scanning.
				colfmt = "g"
			}
			if len(strings.Fields(colVal)) != 1 {
				// Scanf functions tokenize by space.
				return r.wrapError(fmt.Errorf("value with fmt option can not have whitespace"), col)
			}
			var (
				typ1   = typ.FieldByIndex(col.fieldIdx).Type
				p1     = unsafe.Pointer(uintptr(p) + col.offset)
				v      = reflect.NewAt(typ1, p1).Interface()
				n, err = fmt.Sscanf(colVal, "%"+colfmt, v)
			)
			if err != nil {
				return r.wrapError(err, col)
			}
			if n != 1 {
				return r.wrapError(fmt.Errorf("%d objects scanned for %s; expected 1", n, colVal), col)
			}
			continue
		}
		switch col.kind {
		case reflect.Bool:
			var v bool
			switch colVal {
			case "Y", "yes":
				v = true
			case "N", "no":
				v = false
			default:
				var err error
				if v, err = strconv.ParseBool(colVal); err != nil {
					return r.wrapError(err, col)
				}
			}
			*(*bool)(unsafe.Pointer(uintptr(p) + col.offset)) = v
		case reflect.String:
			*(*string)(unsafe.Pointer(uintptr(p) + col.offset)) = colVal
		case reflect.Int8:
			v, err := strconv.ParseInt(colVal, 0, 8)
			if err != nil {
				return r.wrapError(err, col)
			}
			*(*int8)(unsafe.Pointer(uintptr(p) + col.offset)) = int8(v)
		case reflect.Int16:
			v, err := strconv.ParseInt(colVal, 0, 16)
			if err != nil {
				return r.wrapError(err, col)
			}
			*(*int16)(unsafe.Pointer(uintptr(p) + col.offset)) = int16(v)
		case reflect.Int32:
			v, err := strconv.ParseInt(colVal, 0, 32)
			if err != nil {
				return r.wrapError(err, col)
			}
			*(*int32)(unsafe.Pointer(uintptr(p) + col.offset)) = int32(v)
		case reflect.Int64:
			v, err := strconv.ParseInt(colVal, 0, 64)
			if err != nil {
				return r.wrapError(err, col)
			}
			*(*int64)(unsafe.Pointer(uintptr(p) + col.offset)) = v
		case reflect.Int:
			v, err := strconv.ParseInt(colVal, 0, 64)
			if err != nil {
				return r.wrapError(err, col)
			}
			*(*int)(unsafe.Pointer(uintptr(p) + col.offset)) = int(v)
		case reflect.Uint8:
			v, err := strconv.ParseUint(colVal, 0, 8)
			if err != nil {
				return r.wrapError(err, col)
			}
			*(*uint8)(unsafe.Pointer(uintptr(p) + col.offset)) = uint8(v)
		case reflect.Uint16:
			v, err := strconv.ParseUint(colVal, 0, 16)
			if err != nil {
				return r.wrapError(err, col)
			}
			*(*uint16)(unsafe.Pointer(uintptr(p) + col.offset)) = uint16(v)
		case reflect.Uint32:
			v, err := strconv.ParseUint(colVal, 0, 32)
			if err != nil {
				return r.wrapError(err, col)

			}
			*(*uint32)(unsafe.Pointer(uintptr(p) + col.offset)) = uint32(v)
		case reflect.Uint64:
			v, err := strconv.ParseUint(colVal, 0, 64)
			if err != nil {
				return r.wrapError(err, col)

			}
			*(*uint64)(unsafe.Pointer(uintptr(p) + col.offset)) = v
		case reflect.Uint:
			v, err := strconv.ParseUint(colVal, 0, 64)
			if err != nil {
				return r.wrapError(err, col)
			}
			*(*uint)(unsafe.Pointer(uintptr(p) + col.offset)) = uint(v)

		case reflect.Float32:
			v, err := strconv.ParseFloat(colVal, 32)
			if err != nil {
				return r.wrapError(err, col)

			}
			*(*float32)(unsafe.Pointer(uintptr(p) + col.offset)) = float32(v)
		case reflect.Float64:
			v, err := strconv.ParseFloat(colVal, 64)
			if err != nil {
				return r.wrapError(err, col)

			}
			*(*float64)(unsafe.Pointer(uintptr(p) + col.offset)) = v
		default:
			return r.wrapError(fmt.Errorf("unsupported type %v", col.kind), col)
		}
	}
	return nil
}

// EmptyReadErrStr is the error-string returned by Read() when the file is
// empty, and at least a header line was expected.
const EmptyReadErrStr = "empty file: could not read the header row"

// Read reads the next TSV row into a go struct.  The argument must be a pointer
// to a struct. It parses each column in the row into the matching struct
// fields.
//
// Example:
//   r := tsv.NewReader(...)
//   ...
//   type row struct {
//     Col0 string
//     Col1 int
//     Float int
//  }
//  var v row
//  err := r.Read(&v)
//
//
// If !Reader.HasHeaderRow or !Reader.UseHeaderNames, the N-th column (base
// zero) will be parsed into the N-th field in the struct.
//
// If Reader.HasHeaderRow and Reader.UseHeaderNames, then the struct's field
// name must match one of the column names listed in the first row in the TSV
// input. The contents of the column with the matching name will be parsed
// into the struct field.
//
// By default, the column name is the struct's field name, but you can override
// it by setting `tsv:"columnname"` tag in the field. The struct tag may also
// take an fmt option to specify how to parse the value using the fmt package.
// This is useful for parsing numbers written in a different base. Note that
// not all verbs are supported with the scanning functions in the fmt package.
// Using the fmt option may lead to slower performance.
// Imagine the following row type:
//
//   type row struct {
//      Chr    string `tsv:"chromo"`
//      Start  int    `tsv:"pos"`
//      Length int
//      Score  int    `tsv:"score,fmt=x"`
//   }
//
// and the following TSV file:
//
//   | chromo | Length | pos | score
//   | chr1   | 1000   | 10  | 0a
//   | chr2   | 950    | 20  | ff
//
// The first Read() will return row{"chr1", 10, 1000, 10}.
//
// The second Read() will return row{"chr2", 20, 950, 15}.
func (r *Reader) Read(v interface{}) error {
	if r.nRow == 0 && r.HasHeaderRow {
		headerRow, err := r.Reader.Read()
		if err != nil {
			if err == io.EOF {
				err = errors.E(EmptyReadErrStr)
			}
			return err
		}
		r.nRow++
		r.columnIndex = map[string]int{}
		for i, colName := range headerRow {
			r.columnIndex[colName] = i
		}
	}
	row, err := r.Reader.Read()
	if err != nil {
		return err
	}
	r.nRow++
	typ := reflect.TypeOf(v)
	if typ != r.cachedRowType {
		rowFormat, err := parseRowFormat(typ)
		if err != nil {
			return err
		}
		if r.UseHeaderNames {
			if err = r.validateRowFormat(rowFormat); err != nil {
				return err
			}
		}
		r.cachedRowType = typ
		r.cachedRowFormat = rowFormat
	}
	return r.fillRow(v, row)
}
