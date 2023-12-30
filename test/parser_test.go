package test

import (
	"bytes"
	"context"
	"encoding/csv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/why2go/csv_parser"
)

func TestAllPrimitiveType(t *testing.T) {
	type Demo struct {
		Bool    bool    `csv:"bool"`
		Int     int     `csv:"int"`
		Int8    int8    `csv:"int8"`
		Int16   int16   `csv:"int16"`
		Int32   int32   `csv:"int32"`
		Int64   int64   `csv:"int64"`
		Uint    uint    `csv:"uint"`
		Uint8   uint8   `csv:"uint8"`
		Uint16  uint16  `csv:"uint16"`
		Uint32  uint32  `csv:"uint32"`
		Uint64  uint64  `csv:"uint64"`
		Float32 float32 `csv:"float32"`
		Float64 float64 `csv:"float64"`
		String  string  `csv:"string"`
		Ignored string  `csv:"-"`
	}

	data := `bool,int,int8,int16,int32,int64,uint,uint8,uint16,uint32,uint64,float32,float64,string
true,100,108,1016,1032,1064,100,108,1016,1032,1064,100.32,100.64,hello world
`
	var err error
	r := csv.NewReader(bytes.NewBufferString(data))

	parser, err := csv_parser.NewCsvParser[Demo](r)
	assert.Empty(t, err)
	defer parser.Close()

	for dw := range parser.DataChan(context.Background()) {
		assert.Empty(t, dw.Err)
		assert.Equal(t, dw.Data, &Demo{true, 100, 108, 1016, 1032, 1064, 100, 108, 1016, 1032, 1064, 100.32, 100.64, "hello world", ""})
	}
}

func TestSlice(t *testing.T) {
	type Demo struct {
		Nums []int64 `csv:"nums"`
	}
	data := `[[nums]],[[nums]],[[nums]]
1,2,
4,5,6
7,8,9
10,,12
`

	r := csv.NewReader(bytes.NewBufferString(data))
	parser, err := csv_parser.NewCsvParser[Demo](r)
	assert.Empty(t, err)
	defer parser.Close()

	var demos []*Demo
	for dw := range parser.DataChan(context.Background()) {
		assert.Empty(t, dw.Err)
		demos = append(demos, dw.Data)
	}

	expectedDemoes := []*Demo{
		{Nums: []int64{1, 2, 0}},
		{Nums: []int64{4, 5, 6}},
		{Nums: []int64{7, 8, 9}},
		{Nums: []int64{10, 0, 12}},
	}

	assert.Equal(t, len(demos), len(expectedDemoes))
	for i := range demos {
		assert.Equal(t, demos[i], expectedDemoes[i])
	}
}

func TestSliceAndMap(t *testing.T) {
	data := `name,{{attri:age}},{{attri:height}},[[msg]],[[msg]]
Alice,20,180,"Hi, I'm Alice.",Nice to meet you!
Bob,21,175,"Hi, I'm Bob.",Nice to meet you!
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

	var demoes []*Demo
	for dw := range parser.DataChan(context.Background()) {
		assert.Empty(t, dw.Err)
		demoes = append(demoes, dw.Data)
	}

	expectedDemoes := []*Demo{
		{"Alice", map[string]int16{"age": 20, "height": 180}, []string{"Hi, I'm Alice.", "Nice to meet you!"}},
		{"Bob", map[string]int16{"age": 21, "height": 175}, []string{"Hi, I'm Bob.", "Nice to meet you!"}},
	}

	assert.Equal(t, len(demoes), len(expectedDemoes))
	for i := range demoes {
		assert.Equal(t, demoes[i], expectedDemoes[i])
	}
}

func TestDefaultAndIgnoreFieldError(t *testing.T) {
	data := `name,age
Alice,20
Bob,hello
Candy,
,
`
	type Demo struct {
		Name string `csv:"name,required,default=ziyi" json:"name"`
		Age  int    `csv:"age,default=18" json:"age"`
	}
	r := csv.NewReader(bytes.NewBufferString(data))
	parser, err := csv_parser.NewCsvParser[Demo](r, csv_parser.WithIgnoreFieldParseError[Demo](true))
	assert.Empty(t, err)
	defer parser.Close()

	var demoes []*Demo

	for dw := range parser.DataChan(context.Background()) {
		assert.Empty(t, dw.Err)
		demoes = append(demoes, dw.Data)
	}

	expectedDemoes := []*Demo{
		{"Alice", 20},
		{"Bob", 18},
		{"Candy", 18},
		{"ziyi", 18},
	}

	assert.Equal(t, len(demoes), len(expectedDemoes))
	for i := range demoes {
		assert.Equal(t, demoes[i], expectedDemoes[i])
	}

}
