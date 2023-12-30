# csv_parser
Csv parser can read from csv file and parse records to golang structs.

Csv parser won't read all records with a single method call, instead it reads from a channel, therefore you can limit the memory usage at a low level.



## Getting start

### Installation

```
go get github.com/why2go/csv_parser
```

### Supported field types

1. primitive types: bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string.
2. slice: whose element's type is primitive type.
3. map: whose key's type is string and value's type is primitive type.

Struct fields don't match the above type will be ignored.

### Supported Tags

```go
type Demo struct {
    Name  string            `csv:"name,required,default=unknown" json:"name"`
    Attri map[string]int16  `csv:"attri" json:"attri"`
    Msg   []string          `csv:"msg" json:"msg"`
}
```

As shown above, you can specify tags for csv parser when define a struct.

Csv tag options are sepereated by comma. The first tag part will be treated as the header name, which will be used to match the csv file content. `required` option means the csv file headers must contain this header name, otherwise an error will be returned. `default` option can be used to assign a default value to the field when no value is provieded in the file. If the value text provided in the file can't be transformed to the struct field type, and you choose to ignore the parse error when you create the parser instance with `WithIgnoreFieldParseError` option, default value will be assigned to the struct field as well.

Default option is not applicable for `Slice` and `Map` type.

### Example

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
Alice,20,,"Hi, I'm Alice.",Nice to meet you!
Bob,21,175,"Hi, I'm Bob.",Nice to meet you!
Candy,,189,"Hi, I'm Candy.",Nice to meet you!
,23,172,"Hi, I'm David.",
`
    type Demo struct {
        Name  string            `csv:"name,required,default=unknown" json:"name"`
        Attri map[string]int16  `csv:"attri" json:"attri"`
        Msg   []string          `csv:"msg" json:"msg"`
    }

    r := csv.NewReader(bytes.NewBufferString(data))
    parser, err := csv_parser.NewCsvParser[Demo](r)  // create a csv parser
    if err != nil {
        panic(err)
    }
    defer parser.Close()  // close the parser

    // get parsed data from channel
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
```
