package test

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"

	"github.com/why2go/csv_parser"
)

func Example_parse() {
	data := `name,{{attri:age}},{{attri:height}},[[msg]],[[msg]]
Alice,20,,"Hi, I'm Alice.",Nice to meet you!
Bob,21,175,"Hi, I'm Bob.",Nice to meet you!
Candy,,189,"Hi, I'm Candy.",Nice to meet you!
David,23,172,"Hi, I'm David.",Nice to meet you!
`
	type Demo struct {
		Name  string            `csv:"name,required" json:"name"`
		Attri map[string]*int16 `csv:"attri" json:"attri"`
		Msg   []string          `csv:"msg" json:"msg"`
	}

	r := csv.NewReader(bytes.NewBufferString(data))
	parser, err := csv_parser.NewCsvParser[Demo](r)
	if err != nil {
		panic(err)
	}
	defer parser.Close()

	for dataWrapper := range parser.DataChan(context.Background()) {
		if err != nil {
			panic(err)
		}
		b, err := json.Marshal(dataWrapper.Data)
		if err != nil {
			panic(err)
		}

		fmt.Printf("demo: %s\n", string(b))
	}

	fmt.Printf("done\n")
}
