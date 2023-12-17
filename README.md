# csv_parser
read csv file and parse data to golang structs.

types supported are as follows:

- bool and *bool
- int, int8, int16, int32, int64 and their pointer type
- uint, uint8, uint16, uint32, uint64 and their pointer type
- float32, float64 and their pointer type
- slice whose element type is one of above primitives
- map whose key type is string and element type is one of above primitives



To start with, add dependency:

`go get github.com/why2go/csv_parser`

then write similar code as follows:

```go
package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"

	"github.com/why2go/csv_parser"
)

func main() {
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
	if err != nil {
		panic(err)
	}
    defer parser.Close() // avoid goroutine leak

	fmt.Printf("parser.FieldHeaders(): %v\n", parser.FieldHeaders())

	fmt.Printf("parser.FileHeaders(): %v\n", parser.FileHeaders())

    // get data from channel
	for dataWrapper := range parser.DataChan(context.Background()) {
		if dataWrapper.Err != nil {
			panic(dataWrapper.Err)
		}
		b, _ := json.Marshal(dataWrapper.Data)

		fmt.Printf("demo: %s\n", string(b))
	}

	fmt.Printf("done\n")
}
```

