package test

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/why2go/csv_parser"
)

type Demo struct {
	Bool       bool     `csv:"bool"`
	BoolPtr    *bool    `csv:"boolPtr"`
	Int        int      `csv:"int"`
	IntPtr     *int     `csv:"intPtr"`
	Int8       int8     `csv:"int8"`
	Int8Ptr    *int8    `csv:"int8Ptr"`
	Int16      int16    `csv:"int16"`
	Int16Ptr   *int16   `csv:"int16Ptr"`
	Int32      int32    `csv:"int32"`
	Int32Ptr   *int32   `csv:"int32Ptr"`
	Int64      int64    `csv:"int64"`
	Int64Ptr   *int64   `csv:"int64Ptr"`
	Uint       uint     `csv:"uint"`
	UintPtr    *uint    `csv:"uintPtr"`
	Uint8      uint8    `csv:"uint8"`
	Uint8Ptr   *uint8   `csv:"uint8Ptr"`
	Uint16     uint16   `csv:"uint16"`
	Uint16Ptr  *uint16  `csv:"uint16Ptr"`
	Uint32     uint32   `csv:"uint32"`
	Uint32Ptr  *uint32  `csv:"uint32Ptr"`
	Uint64     uint64   `csv:"uint64"`
	Uint64Ptr  *uint64  `csv:"uint64Ptr"`
	Float32    float32  `csv:"float32"`
	Float32Ptr *float32 `csv:"float32Ptr"`
	Float64    float64  `csv:"float64"`
	Float64Ptr *float64 `csv:"float64Ptr"`
	String     string   `csv:"string"`
	StringPtr  *string  `csv:"stringPtr"`
	Ignored    string   `csv:"-"`
}

func TestAllPrimitiveType(t *testing.T) {
	data := `bool,boolPtr,int,intPtr,int8,int8Ptr,int16,int16Ptr,int32,int32Ptr,int64,int64Ptr,uint,uintPtr,uint8,uint8Ptr,uint16,uint16Ptr,uint32,uint32Ptr,uint64,uint64Ptr,float32,float32Ptr,float64,float64Ptr,string,stringPtr
1,true,1234,1234,127,127,1234,1234,1234,1234,1234,1234,12345,12345,255,255,65535,65535,12345,12345,12345,12345,123.45,123.45,123.45,123.456,hello world,world hello`
	var err error
	r := csv.NewReader(bytes.NewBufferString(data))

	parser, err := csv_parser.NewCsvParser[Demo](r)
	assert.Empty(t, err)
	defer parser.Close()

	for dataWrapper := range parser.DataChan(context.Background()) {
		assert.Empty(t, dataWrapper.Err)
		b, err := json.Marshal(dataWrapper.Data)
		assert.Empty(t, err)

		fmt.Printf("demo: %s\n", string(b))
	}

	fmt.Printf("done\n")
}

func TestSlice(t *testing.T) {
	data := `[[nums]],[[nums]],[[nums]]
1,2,
4,5,6
7,8,9
10,,12
`
	type Demo struct {
		Nums []*int64 `csv:"nums"`
	}

	r := csv.NewReader(bytes.NewBufferString(data))

	parser, err := csv_parser.NewCsvParser[Demo](r)
	assert.Empty(t, err)
	defer parser.Close()

	for dataWrapper := range parser.DataChan(context.Background()) {
		assert.Empty(t, dataWrapper.Err)
		b, err := json.Marshal(dataWrapper.Data)
		assert.Empty(t, err)

		fmt.Printf("demo: %s\n", string(b))
	}

	fmt.Printf("done\n")
}

func TestMap(t *testing.T) {
	data := `name,{{attri:age}},{{attri:height}},[[msg]],[[msg]]
Alice,20,180,"Hi, I'm Alice.",Nice to meet you!
Bob,21,175,"Hi, I'm Bob.",Nice to meet you!
Candy,22,189,"Hi, I'm Candy.",Nice to meet you!
David,23,172,"Hi, I'm David.",Nice to meet you!
`
	type Demo struct {
		Name  string           `csv:"name" json:"name"`
		Attri map[string]int16 `csv:"attri" json:"attri"`
		Msg   []string         `csv:"msg" json:"msg"`
	}
	r := csv.NewReader(bytes.NewBufferString(data))

	parser, err := csv_parser.NewCsvParser[Demo](r)
	assert.Empty(t, err)
	defer parser.Close()

	fmt.Printf("parser.FieldHeaders(): %v\n", parser.FieldHeaders())

	fmt.Printf("parser.FileHeaders(): %v\n", parser.FileHeaders())

	for dataWrapper := range parser.DataChan(context.Background()) {
		assert.Empty(t, dataWrapper.Err)
		b, err := json.Marshal(dataWrapper.Data)
		assert.Empty(t, err)

		fmt.Printf("demo: %s\n", string(b))
	}

	fmt.Printf("done\n")
}

func TestNil(t *testing.T) {
	data := `name,{{attri:age}},{{attri:height}},[[msg]],[[msg]],num
Alice,,180,"Hi, I'm Alice.",Nice to meet you!,
Bob,21,175,"Hi, I'm Bob.",,2
,22,189,"Hi, I'm Candy.",Nice to meet you!,3
 ,23,,"Hi, I'm David.",Nice to meet you!,
`
	type Demo struct {
		Name  *string          `csv:"name" json:"name"`
		Attri map[string]int16 `csv:"attri" json:"attri"`
		Msg   []*string        `csv:"msg" json:"msg"`
		Num   *int16           `csv:"num" json:"num"`
	}
	r := csv.NewReader(bytes.NewBufferString(data))

	parser, err := csv_parser.NewCsvParser[Demo](r)
	assert.Empty(t, err)
	defer parser.Close()

	fmt.Printf("parser.FieldHeaders(): %v\n", parser.FieldHeaders())

	fmt.Printf("parser.FileHeaders(): %v\n", parser.FileHeaders())

	for dataWrapper := range parser.DataChan(context.Background()) {
		assert.Empty(t, dataWrapper.Err)
		b, err := json.Marshal(dataWrapper.Data)
		assert.Empty(t, err)

		fmt.Printf("demo: %s\n", string(b))
	}

	fmt.Printf("done\n")
}

func TestIgnoreFieldError(t *testing.T) {
	data := `name,age
Alice,20
Bob,hello

`
	type Demo struct {
		Name string `csv:"name" json:"name"`
		Age  int    `csv:"age" json:"age"`
	}
	r := csv.NewReader(bytes.NewBufferString(data))

	parser, err := csv_parser.NewCsvParser[Demo](r, csv_parser.WithIgnoreFieldParseError[Demo](true))
	assert.Empty(t, err)
	defer parser.Close()

	fmt.Printf("parser.FieldHeaders(): %v\n", parser.FieldHeaders())

	fmt.Printf("parser.FileHeaders(): %v\n", parser.FileHeaders())

	for dataWrapper := range parser.DataChan(context.Background()) {
		assert.Empty(t, dataWrapper.Err)
		b, err := json.Marshal(dataWrapper.Data)
		assert.Empty(t, err)

		fmt.Printf("demo: %s\n", string(b))
	}

	fmt.Printf("parser.GetLineCursor(): %v\n", parser.GetLineCursor())

	fmt.Printf("done\n")
}
