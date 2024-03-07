package tsv_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/grailbio/base/tsv"
	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/expect"
)

func TestReadBool(t *testing.T) {
	read := func(data string) bool {
		type row struct {
			Col0 bool
		}
		r := tsv.NewReader(bytes.NewReader([]byte("col0\n" + data)))
		r.HasHeaderRow = true
		var v row
		expect.NoError(t, r.Read(&v))
		return v.Col0
	}

	expect.True(t, read("true"))
	expect.False(t, read("false"))
	expect.True(t, read("Y"))
	expect.True(t, read("yes"))
	expect.False(t, read("N"))
	expect.False(t, read("no"))
}

func TestReadInt(t *testing.T) {
	newReader := func() *tsv.Reader {
		r := tsv.NewReader(bytes.NewReader([]byte(`col0	col1
0	0.5
`)))
		r.HasHeaderRow = true
		return r
	}

	{
		type row struct {
			Col0 int8
			Col1 float32
		}
		r := newReader()
		var v row
		expect.NoError(t, r.Read(&v))
		expect.EQ(t, v, row{0, 0.5})
	}

	{
		type row struct {
			Col0 int16
			Col1 float64
		}
		r := newReader()
		var v row
		expect.NoError(t, r.Read(&v))
		expect.EQ(t, v, row{0, 0.5})
	}

	{
		type row struct {
			Col0 int32
			Col1 float64
		}
		r := newReader()
		var v row
		expect.NoError(t, r.Read(&v))
		expect.EQ(t, v, row{0, 0.5})
	}
	{
		type row struct {
			Col0 int64
			Col1 float64
		}
		r := newReader()
		var v row
		expect.NoError(t, r.Read(&v))
		expect.EQ(t, v, row{0, 0.5})
	}
	{
		type row struct {
			Col0 int
			Col1 float64
		}
		r := newReader()
		var v row
		expect.NoError(t, r.Read(&v))
		expect.EQ(t, v, row{0, 0.5})
	}
	{
		type row struct {
			Col0 uint8
			Col1 float32
		}
		r := newReader()
		var v row
		expect.NoError(t, r.Read(&v))
		expect.EQ(t, v, row{0, 0.5})
	}

	{
		type row struct {
			Col0 uint16
			Col1 float64
		}
		r := newReader()
		var v row
		expect.NoError(t, r.Read(&v))
		expect.EQ(t, v, row{0, 0.5})
	}

	{
		type row struct {
			Col0 uint32
			Col1 float64
		}
		r := newReader()
		var v row
		expect.NoError(t, r.Read(&v))
		expect.EQ(t, v, row{0, 0.5})
	}
}

func TestReadFmt(t *testing.T) {
	r := tsv.NewReader(bytes.NewReader([]byte(`"""helloworld"""	05.20	true	0a`)))
	type row struct {
		ColA string  `tsv:",fmt=q"`
		ColB float64 `tsv:",fmt=1.2f"`
		ColC bool    `tsv:",fmt=t"`
		ColD int     `tsv:",fmt=x"`
	}
	var v row
	assert.NoError(t, r.Read(&v))
	assert.EQ(t, v, row{`helloworld`, 5.2, true, 10})
}

func TestReadFmtWithSpace(t *testing.T) {
	r := tsv.NewReader(bytes.NewReader([]byte(`"hello world"`)))
	type row struct {
		ColA string `tsv:",fmt=s"`
	}
	var v row
	expect.Regexp(t, r.Read(&v), "value with fmt option can not have whitespace")
}

func TestReadWithoutHeader(t *testing.T) {
	type row struct {
		ColA string
		ColB int
	}
	r := tsv.NewReader(bytes.NewReader([]byte(`key1	2
key2	3
`)))
	var v row
	assert.NoError(t, r.Read(&v))
	expect.EQ(t, v, row{"key1", 2})
	assert.NoError(t, r.Read(&v))
	expect.EQ(t, v, row{"key2", 3})
	assert.EQ(t, r.Read(&v), io.EOF)
}

func TestReadSkipUnexportedFields(t *testing.T) {
	type row struct {
		colA string
		colB int
		ColC int `tsv:"col0"`
	}
	r := tsv.NewReader(bytes.NewReader([]byte(`key	col0	col1
key0	1	0.5
key1	2	1.5
`)))
	r.HasHeaderRow = true
	r.UseHeaderNames = true
	var v row
	assert.NoError(t, r.Read(&v))
	expect.EQ(t, v, row{"", 0, 1})
	assert.NoError(t, r.Read(&v))
	expect.EQ(t, v, row{"", 0, 2})
	assert.EQ(t, r.Read(&v), io.EOF)
}

func TestReadEmbeddedStruct(t *testing.T) {
	type embedded1 struct {
		Col1 int     `tsv:"col1"`
		Col2 float64 `tsv:"col2_2,fmt=0.3f"`
	}
	type embedded2 struct {
		Col2 float32 `tsv:"col2_1"`
	}
	type row struct {
		Key string `tsv:"key"`
		embedded1
		embedded2
	}
	r := tsv.NewReader(bytes.NewReader([]byte(`key	col2_1	col1	col2_2
key0	0.5	1	0.123
key1	1.5	2	0.789
`)))
	r.HasHeaderRow = true
	r.UseHeaderNames = true
	var v row
	assert.NoError(t, r.Read(&v))
	expect.EQ(t, v, row{"key0", embedded1{1, 0.123}, embedded2{0.5}})
	assert.NoError(t, r.Read(&v))
	expect.EQ(t, v, row{"key1", embedded1{2, 0.789}, embedded2{1.5}})
	assert.EQ(t, r.Read(&v), io.EOF)
}

func TestReadExtraColumns(t *testing.T) {
	type row struct {
		ColA string
		ColB int
	}
	r := tsv.NewReader(bytes.NewReader([]byte(`key1	2	22
key2	3	33
`)))
	r.RequireParseAllColumns = true
	var v row
	expect.Regexp(t, r.Read(&v), "extra columns found")
}

func TestReadDisallowExtraNamedColumns(t *testing.T) {
	type row struct {
		ColA string
		ColB int
	}
	r := tsv.NewReader(bytes.NewReader([]byte(`ColA	ColB	ColC
key1	2	22
key2	3	33
`)))
	r.HasHeaderRow = true
	r.UseHeaderNames = true
	r.RequireParseAllColumns = true
	var v row
	expect.Regexp(t, r.Read(&v), "number of columns found")
}

func TestReadMissingColumns(t *testing.T) {
	type row struct {
		ColA string
		ColB int
	}
	r := tsv.NewReader(bytes.NewReader([]byte(`ColA
key1
key2
`)))
	r.HasHeaderRow = true
	r.UseHeaderNames = true
	r.RequireParseAllColumns = true
	var v row
	expect.Regexp(t, r.Read(&v), "number of columns found")
}

func TestReadMismatchedColumns(t *testing.T) {
	type row struct {
		ColA string
		ColB int
	}
	r := tsv.NewReader(bytes.NewReader([]byte(`ColA	ColC
key1	2
key2	3
`)))
	r.HasHeaderRow = true
	r.UseHeaderNames = true
	r.RequireParseAllColumns = true
	var v row
	expect.Regexp(t, r.Read(&v), "does not appear in the header")
}

func TestReadPartialStruct(t *testing.T) {
	type row struct {
		ColA string
		ColB int
	}
	r := tsv.NewReader(bytes.NewReader([]byte(`ColA
key1
key2
`)))
	r.HasHeaderRow = true
	r.UseHeaderNames = true
	r.RequireParseAllColumns = true
	r.IgnoreMissingColumns = true
	var v row
	assert.NoError(t, r.Read(&v))
	expect.EQ(t, v, row{"key1", 0})
	assert.NoError(t, r.Read(&v))
	expect.EQ(t, v, row{"key2", 0})
	assert.EQ(t, r.Read(&v), io.EOF)
}

func TestReadAllowExtraNamedColumns(t *testing.T) {
	type row struct {
		ColB int
		ColA string
	}
	r := tsv.NewReader(bytes.NewReader([]byte(`ColA	ColB	ColC
key1	2	22
key2	3	33
`)))
	r.HasHeaderRow = true
	r.UseHeaderNames = true
	var v row
	expect.NoError(t, r.Read(&v))
	expect.EQ(t, v, row{2, "key1"})
	expect.NoError(t, r.Read(&v))
	expect.EQ(t, v, row{3, "key2"})
}

func TestReadParseError(t *testing.T) {
	type row struct {
		ColA int    `tsv:"cola"`
		ColB string `tsv:"colb"`
	}
	r := tsv.NewReader(bytes.NewReader([]byte(`key1	2
`)))
	var v row
	expect.Regexp(t, r.Read(&v), `line 1, column 0, 'cola' \(Go field 'ColA'\):`)
}

func TestReadValueError(t *testing.T) {
	type row struct {
		ColA string
		ColB int
	}
	r := tsv.NewReader(bytes.NewReader([]byte(`key1	2
key2	3
`)))
	var v int
	expect.Regexp(t, r.Read(&v), `destination must be a pointer to struct, but found \*int`)
	expect.Regexp(t, r.Read(v), `destination must be a pointer to struct, but found int`)
}

func TestReadMultipleRowTypes(t *testing.T) {
	r := tsv.NewReader(bytes.NewReader([]byte(`key1	2
3	key2
`)))
	{
		type row struct {
			ColA string
			ColB int
		}
		var v row
		assert.NoError(t, r.Read(&v))
		expect.EQ(t, v, row{"key1", 2})
	}
	{
		type row struct {
			ColA int
			ColB string
		}
		var v row
		assert.NoError(t, r.Read(&v))
		expect.EQ(t, v, row{3, "key2"})
	}
}

func ExampleReader() {
	type row struct {
		Key  string
		Col0 uint
		Col1 float64
	}

	readRow := func(r *tsv.Reader) row {
		var v row
		if err := r.Read(&v); err != nil {
			panic(err)
		}
		return v
	}

	r := tsv.NewReader(bytes.NewReader([]byte(`Key	Col0	Col1
key0	0	0.5
key1	1	1.5
`)))
	r.HasHeaderRow = true
	r.UseHeaderNames = true
	fmt.Printf("%+v\n", readRow(r))
	fmt.Printf("%+v\n", readRow(r))

	var v row
	if err := r.Read(&v); err != io.EOF {
		panic(err)
	}
	// Output:
	// {Key:key0 Col0:0 Col1:0.5}
	// {Key:key1 Col0:1 Col1:1.5}
}

func ExampleReader_withTag() {
	type row struct {
		ColA    string  `tsv:"key"`
		ColB    float64 `tsv:"col1"`
		Skipped int     `tsv:"-"`
		ColC    int     `tsv:"col0,fmt=d"`
		Hex     int     `tsv:",fmt=x"`
		Hyphen  int     `tsv:"-,"`
	}
	readRow := func(r *tsv.Reader) row {
		var v row
		if err := r.Read(&v); err != nil {
			panic(err)
		}
		return v
	}

	r := tsv.NewReader(bytes.NewReader([]byte(`key	col0	col1	Hex	-
key0	0	0.5	a	1
key1	1	1.5	f	2
`)))
	r.HasHeaderRow = true
	r.UseHeaderNames = true
	fmt.Printf("%+v\n", readRow(r))
	fmt.Printf("%+v\n", readRow(r))

	var v row
	if err := r.Read(&v); err != io.EOF {
		panic(err)
	}
	// Output:
	// {ColA:key0 ColB:0.5 Skipped:0 ColC:0 Hex:10 Hyphen:1}
	// {ColA:key1 ColB:1.5 Skipped:0 ColC:1 Hex:15 Hyphen:2}
}

func BenchmarkReader(b *testing.B) {
	b.StopTimer()
	const nRow = 10000
	data := bytes.Buffer{}
	for i := 0; i < nRow; i++ {
		data.WriteString(fmt.Sprintf("key%d\t%d\t%f\n", i, i, float64(i)+0.5))
	}
	b.StartTimer()

	type row struct {
		Key   string
		Int   int
		Float float64
	}
	for i := 0; i < b.N; i++ {
		r := tsv.NewReader(bytes.NewReader(data.Bytes()))
		var (
			val row
			n   int
		)
		for {
			err := r.Read(&val)
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}
			n++
		}
		assert.EQ(b, n, nRow)
	}
}
