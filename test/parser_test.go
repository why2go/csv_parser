package test

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
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

func TestAllType(t *testing.T) {
	var err error
	f, err := os.Open("./type_data.csv")
	assert.Empty(t, err)
	r := csv.NewReader(f)

	parser, err := csv_parser.NewCsvParser[Demo](r)
	assert.Empty(t, err)

	for dataWrapper := range parser.DataChan(context.Background()) {
		assert.Empty(t, dataWrapper.Err)
		b, err := json.Marshal(dataWrapper.Data)
		assert.Empty(t, err)

		fmt.Printf("demo: %s\n", string(b))
	}

	fmt.Printf("done\n")
}
