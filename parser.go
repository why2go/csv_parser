// Copyright 2023 Sun Zhi. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package csv_parser

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

const (
	FieldTagKey          = "csv"      // the key to identify the csv tag
	FieldTagOpt_Omit     = "-"        // if the field tag is "-", the field is always omitted
	FieldTagOpt_Required = "required" // if the tag present, the field must be presented in the csv file header
)

var (
	// if a struct field type is slice, corresponding csv file header must match this pattern, for example: [[nums]]
	sliceRegex = regexp.MustCompile(`^\[\[[\w:-]+\]\]$`)
	// if a struct field type is map, corresponding csv file header must match this pattern, for example: {{map1:key1}}
	mapRegex = regexp.MustCompile(`^{{[\w-]+:[\w-:]+}}$`)
)

type fieldHeader struct {
	name       string       // corresponding csv file header name
	fieldIndex int          // the index of the field in the struct
	fieldType  reflect.Type // the type of the field
	required   bool         // has "required" option or not
}

func (fh *fieldHeader) String() string {
	return fmt.Sprintf("{name: %s, fieldIndex: %d, fieldType: %v, required: %v}", fh.name, fh.fieldIndex, fh.fieldType, fh.required)
}

// the csv file header
type fileHeader struct {
	fullName   string // original csv header name
	name       string // csv header name without prefix and suffix
	matchSlice bool   // match slice pattern or not
	matchMap   bool   // match map pattern or not
	mapName    string // if the header match map pattern, this field records the map field tag name
	mapKey     string // if the header match map pattern, this field records the key of the map element
}

func (fh *fileHeader) String() string {
	return fmt.Sprintf("{name: %s, fullName: %s}", fh.name, fh.fullName)
}

type CsvParser[T any] struct {
	err                   error
	reader                *csv.Reader
	fileHeaders           []fileHeader
	fieldHeaders          map[string]*fieldHeader
	dataChan              chan *DataWrapper[T]
	targetStructType      reflect.Type
	doParseOnce           sync.Once
	closeCh               chan bool // to avoid goroutine leaking
	ignoreFieldParseError bool      // if set true, the parsing process will continue when some field can't be parsed
	lineCursor            int       // point to the next line to be parsed, starting from one
}

// the data structure in the data channel.
//
// if error occurs, the Err field is not nil, and parsing process will be stopped
type DataWrapper[T any] struct {
	Data *T
	Err  error
}

// Create a CsvParser instance, type T must be struct, can't be a pointer to struct.
// The reader is an instance of encoding/csv.Reader from go standard library.
// Type of T's fields supports the following types:
// 1. primitive types: bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string.
// 2. pointer of primitive types.
// 3. slice: whose element's type is primitive type or pointer of primitive type.
// 4. map: whose key's type is string and value's type is primitive type or pointer of primitive type.
func NewCsvParser[T any](reader *csv.Reader, opts ...NewParserOption[T]) (parser *CsvParser[T], err error) {
	if reader == nil {
		return nil, errors.New("csv reader is nil")
	}
	parser = &CsvParser[T]{
		reader:           reader,
		fieldHeaders:     make(map[string]*fieldHeader),
		dataChan:         make(chan *DataWrapper[T]),
		targetStructType: reflect.TypeOf(new(T)).Elem(),
		closeCh:          make(chan bool),
		lineCursor:       1,
	}
	for i := range opts {
		opts[i](parser)
	}

	err = parser.getStructHeaders()
	if err != nil {
		parser.err = err
		close(parser.dataChan)
		return nil, err
	}

	err = parser.getFileHeaders()
	if err != nil {
		parser.err = err
		close(parser.dataChan)
		return nil, err
	}

	err = parser.validateHeaders()
	if err != nil {
		parser.err = err
		close(parser.dataChan)
		return nil, err
	}

	return parser, nil
}

type NewParserOption[T any] func(*CsvParser[T])

func WithIgnoreFieldParseError[T any](b bool) NewParserOption[T] {
	return func(cp *CsvParser[T]) {
		cp.ignoreFieldParseError = b
	}
}

// stop the parsing process and close the data channel.
// you should call this method when task finished or aborted.
func (parser *CsvParser[T]) Close() error {
	close(parser.closeCh)
	return nil
}

// if parsing failed, return the parsing error
func (parser *CsvParser[T]) Error() error {
	return parser.err
}

func (parser *CsvParser[T]) FieldHeaders() []string {
	var hds []string
	for _, v := range parser.fieldHeaders {
		hds = append(hds, v.String())
	}
	return hds
}

func (parser *CsvParser[T]) FileHeaders() []string {
	var hds []string
	for _, v := range parser.fileHeaders {
		hds = append(hds, v.String())
	}
	return hds
}

func (parser *CsvParser[T]) getStructHeaders() (err error) {
	t := parser.targetStructType
	if t.Kind() != reflect.Struct {
		err = fmt.Errorf("type of T must be a struct, but we got %s", t.Kind().String())
		return err
	}

	headerNameSet := make(map[string]struct{}) // used to check duplicate field tags
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}
		if parser.isSupportedStructFieldType(sf.Type) {
			header := new(fieldHeader)
			header.fieldIndex = i
			header.fieldType = sf.Type
			if tag, ok := sf.Tag.Lookup(FieldTagKey); ok {
				name, opts, _ := strings.Cut(tag, ",")
				name = strings.TrimSpace(name)
				if name == FieldTagOpt_Omit { // ignore this field
					continue
				}
				if _, ok := headerNameSet[name]; ok {
					return fmt.Errorf("duplicate struct header name: %s", name)
				} else {
					headerNameSet[name] = struct{}{}
				}
				header.name = name
				header.required = strings.Contains(opts, FieldTagOpt_Required)
			}
			// default header name is the field name
			if len(header.name) == 0 {
				header.name = sf.Name
			}
			parser.fieldHeaders[header.name] = header
		}
	}
	if len(parser.fieldHeaders) == 0 {
		return errors.New("no struct header found")
	}
	return nil
}

func (parser *CsvParser[T]) isSupportedStructFieldType(typ reflect.Type) bool {
	isPrimitiveType := func(typ reflect.Type) bool {
		if typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		return typ.Kind() == reflect.String ||
			typ.Kind() == reflect.Int ||
			typ.Kind() == reflect.Int8 ||
			typ.Kind() == reflect.Int16 ||
			typ.Kind() == reflect.Int32 ||
			typ.Kind() == reflect.Int64 ||
			typ.Kind() == reflect.Uint ||
			typ.Kind() == reflect.Uint8 ||
			typ.Kind() == reflect.Uint16 ||
			typ.Kind() == reflect.Uint32 ||
			typ.Kind() == reflect.Uint64 ||
			typ.Kind() == reflect.Float32 ||
			typ.Kind() == reflect.Float64 ||
			typ.Kind() == reflect.Bool
	}

	return isPrimitiveType(typ) ||
		(typ.Kind() == reflect.Slice && isPrimitiveType(typ.Elem())) ||
		(typ.Kind() == reflect.Map && typ.Key().Kind() == reflect.String && isPrimitiveType(typ.Elem()))

}

func (parser *CsvParser[T]) getFileHeaders() error {
	var err error
	record, err := parser.reader.Read()
	parser.lineCursor++
	if err != nil {
		return err
	}
	for i := range record {
		fullName := strings.TrimSpace(record[i])
		fh := fileHeader{fullName: fullName}
		if sliceRegex.MatchString(fullName) { // match slice pattern
			fh.name = fullName[2 : len(fullName)-2]
			fh.matchSlice = true
		} else if mapRegex.MatchString(fullName) { // match map pattern
			fh.name = fullName[2 : len(fullName)-2]
			fh.matchMap = true
			var found bool
			fh.mapName, fh.mapKey, found = strings.Cut(fh.name, ":")
			if !found {
				return fmt.Errorf("malformed map header: %s", fh.fullName)
			}
		} else {
			fh.name = fh.fullName
		}
		parser.fileHeaders = append(parser.fileHeaders, fh)
	}
	// check duplicate headers, headers matching slice pattern will be ignored
	fileHeaderSet := make(map[string]struct{})
	for i := range parser.fileHeaders {
		if parser.fileHeaders[i].matchSlice {
			continue
		}
		if _, ok := fileHeaderSet[parser.fileHeaders[i].name]; ok {
			return fmt.Errorf("duplicate csv file header: %s", parser.fileHeaders[i].fullName)
		} else {
			fileHeaderSet[parser.fileHeaders[i].name] = struct{}{}
		}
	}
	return nil
}

func (parser *CsvParser[T]) validateHeaders() error {
	requiredSet := map[string]struct{}{}
	for _, v := range parser.fieldHeaders {
		if v.required {
			requiredSet[v.name] = struct{}{}
		}
	}

	// check required option
	for i := range parser.fileHeaders {
		delete(requiredSet, parser.fileHeaders[i].name)
	}
	if len(requiredSet) > 0 {
		keys := make([]string, 0, len(requiredSet))
		for k := range requiredSet {
			keys = append(keys, k)
		}
		return fmt.Errorf("some required headers not foun in csv file header: %v", strings.Join(keys, ","))
	}

	// check slice and map fields
	for i := range parser.fileHeaders {
		if parser.fileHeaders[i].matchSlice {
			if hd, ok := parser.fieldHeaders[parser.fileHeaders[i].name]; ok {
				if hd.fieldType.Kind() != reflect.Slice {
					return fmt.Errorf("field %s is not a slice", parser.fileHeaders[i].fullName)
				}
			}
		}
		if parser.fileHeaders[i].matchMap {
			if hd, ok := parser.fieldHeaders[parser.fileHeaders[i].name]; ok {
				if hd.fieldType.Kind() != reflect.Map {
					return fmt.Errorf("field %s is not a map", parser.fileHeaders[i].fullName)
				}
			}
		}
	}

	return nil
}

func (parser *CsvParser[T]) GetLineCursor() int {
	return parser.lineCursor
}

// Return the parsed data channel.
// If some error occurs, dataWrapper.Err will not be nil, and the channel will be closed forever.
func (parser *CsvParser[T]) DataChan(ctx context.Context) <-chan *DataWrapper[T] {
	parser.doParseOnce.Do(func() {
		go func() {
			parser.doParse(ctx)
		}()
	})

	return parser.dataChan
}

func (parser *CsvParser[T]) doParse(ctx context.Context) {
	if parser.err != nil {
		return
	}

	// if parser.err was set, stop parsing process and close the channel
	defer func() {
		if parser.err != nil {
			parser.dataChan <- &DataWrapper[T]{Err: parser.err}
			close(parser.dataChan)
			return
		}
	}()

	for {
		record, err := parser.reader.Read()
		parser.lineCursor++

		if err == io.EOF { // all records were parsed
			close(parser.dataChan)
			return
		}
		if err != nil {
			parser.err = err
			return
		}

		val := reflect.New(parser.targetStructType)

		for j := range record {
			fileHeader := parser.fileHeaders[j]
			var name string
			if fileHeader.matchMap {
				name = fileHeader.mapName
			} else {
				name = fileHeader.name
			}
			if fieldHeader, ok := parser.fieldHeaders[name]; !ok {
				continue // ignore uninteresting field
			} else {
				var (
					fieldType     = fieldHeader.fieldType
					primitiveType = fieldType
					fieldIdx      = fieldHeader.fieldIndex
					fieldVal      = val.Elem().Field(fieldIdx)
					isNil         bool
					val           reflect.Value
					err           error
				)
				// pointer fields
				if fieldType.Kind() == reflect.Pointer {
					primitiveType = fieldType.Elem()
					isNil = shallBeNil(record[j], primitiveType)
				}
				// slice or map
				if fieldType.Kind() == reflect.Slice || fieldType.Kind() == reflect.Map {
					primitiveType = fieldType.Elem()
					if primitiveType.Kind() == reflect.Pointer {
						primitiveType = primitiveType.Elem()
						isNil = shallBeNil(record[j], primitiveType)
						if isNil {
							val = reflect.Zero(fieldType.Elem())
						}
					}
				}

				if !isNil {
					val, err = parsePrimitive(record[j], primitiveType)
					if err != nil {
						if parser.ignoreFieldParseError {
							continue
						} else {
							parser.err = err
							return
						}
					}
				}

				switch fieldType.Kind() {
				case reflect.Slice:
					if !isNil && fieldType.Elem().Kind() == reflect.Pointer {
						v := reflect.New(primitiveType)
						v.Elem().Set(val)
						val = v
					}
					fieldVal.Set(reflect.Append(fieldVal, val))
				case reflect.Map:
					if fieldVal.IsNil() {
						fieldVal.Set(reflect.MakeMap(fieldType))
					}
					if !isNil && fieldType.Elem().Kind() == reflect.Pointer {
						v := reflect.New(primitiveType)
						v.Elem().Set(val)
						val = v
					}
					fieldVal.SetMapIndex(reflect.ValueOf(fileHeader.mapKey), val)
				default:
					if !isNil {
						if fieldType.Kind() == reflect.Pointer {
							v := reflect.New(primitiveType)
							v.Elem().Set(val)
							val = v
						}
						fieldVal.Set(val)
					}
				}
			}
		}

		select {
		case <-ctx.Done():
			parser.err = ctx.Err()
			return
		case <-parser.closeCh:
			parser.err = fmt.Errorf("parser already closed")
			return
		case parser.dataChan <- &DataWrapper[T]{Data: val.Interface().(*T), Err: nil}:
		}
	}
}

func shallBeNil(txt string, typ reflect.Type) bool {
	return (txt == "" && typ.Kind() == reflect.String) ||
		(strings.TrimSpace(txt) == "" && typ.Kind() != reflect.String)
}

func parsePrimitive(txt string, fieldType reflect.Type) (val reflect.Value, err error) {
	var v any
	fieldKind := fieldType.Kind()
	switch fieldKind {
	case reflect.Bool:
		txt := strings.TrimSpace(txt)
		if txt == "true" || txt == "1" {
			v, err = true, nil
		} else if txt != "false" && txt != "0" && txt != "" {
			v, err = nil, fmt.Errorf("unknown bool value: %s", txt)
		} else {
			v, err = false, nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, err = parseInt(strings.TrimSpace(txt), fieldKind)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, err = parseUint(strings.TrimSpace(txt), fieldKind)
	case reflect.Float32, reflect.Float64:
		v, err = parseFloat(strings.TrimSpace(txt), fieldKind)
	case reflect.String:
		v, err = txt, nil
	default:
		v, err = nil, fmt.Errorf("unsupported primitive field type kind, kind: %v", fieldKind.String())
	}
	if err != nil {
		return reflect.Value{}, err
	}
	return reflect.ValueOf(v).Convert(fieldType), nil
}

func parseInt(txt string, kind reflect.Kind) (int64, error) {
	if txt == "" {
		return 0, nil
	}
	var bitSize int
	switch kind {
	case reflect.Int:
		bitSize = 0
	case reflect.Int8:
		bitSize = 8
	case reflect.Int16:
		bitSize = 16
	case reflect.Int32:
		bitSize = 32
	case reflect.Int64:
		bitSize = 64
	default:
		bitSize = 0
	}
	return strconv.ParseInt(txt, 10, bitSize)
}

func parseUint(txt string, kind reflect.Kind) (uint64, error) {
	if txt == "" {
		return 0, nil
	}
	var bitSize int
	switch kind {
	case reflect.Uint:
		bitSize = 0
	case reflect.Uint8:
		bitSize = 8
	case reflect.Uint16:
		bitSize = 16
	case reflect.Uint32:
		bitSize = 32
	case reflect.Uint64:
		bitSize = 64
	default:
		bitSize = 0
	}
	return strconv.ParseUint(txt, 10, bitSize)
}

func parseFloat(txt string, kind reflect.Kind) (float64, error) {
	if txt == "" {
		return 0, nil
	}
	var bitSize int
	switch kind {
	case reflect.Float32:
		bitSize = 32
	case reflect.Float64:
		bitSize = 64
	default:
		bitSize = 32
	}
	return strconv.ParseFloat(txt, bitSize)
}
