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
	FieldTagOpt_Default  = "default"  // default value for the field
)

var (
	// if a struct field type is slice, corresponding csv file header must match this pattern, for example: [[nums]]
	sliceRegex = regexp.MustCompile(`^\[\[[\w:-]+\]\]$`)
	// if a struct field type is map, corresponding csv file header must match this pattern, for example: {{map1:key1}}
	mapRegex = regexp.MustCompile(`^{{[\w-]+:[\w-:]+}}$`)
)

type fieldHeader struct {
	name       string         // corresponding csv file header name
	fieldIndex int            // the index of the field in the struct
	fieldType  reflect.Type   // the type of the field
	required   bool           // has "required" option or not
	defaultVal *reflect.Value // default value for the field, nil if don't have a default value
}

func (fh *fieldHeader) String() string {
	var defaultValStr string
	if fh.defaultVal == nil {
		defaultValStr = "nil"
	} else {
		defaultValStr = fmt.Sprintf("%v", *fh.defaultVal)
	}
	return fmt.Sprintf("{name: %s, fieldIndex: %d, fieldType: %v, required: %v, defaultVal: %#v}", fh.name, fh.fieldIndex, fh.fieldType, fh.required, defaultValStr)
}

func (fh *fieldHeader) hasDefaultVal() bool {
	return fh.defaultVal != nil
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
	closeOnce             sync.Once
	ignoreFieldParseError bool // if set true, the parsing process will continue when some field can't be parsed
	lineCursor            int  // point to the next line to be parsed, index starting from one
}

// DataWrapper is the type of channel data, it wraps parsed record and an error.
// If error occurs, the Err field is not nil, and parsing process will be stopped.
type DataWrapper[T any] struct {
	Data *T
	Err  error
}

// Create a CsvParser instance, type T must be struct, can't be a pointer to struct.
// The reader is an instance of encoding/csv.Reader from go standard library.
// The type of T's fields is constrained to the following kind:
//
//  1. primitive types: bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string.
//
//  2. slice: whose element's type is primitive type.
//
//  3. map: whose key's type is string and value's type is primitive type.
//
// Struct fields don't match the type requirements will be ignored.
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
		lineCursor:       1, // the line number staring from one
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

// If set to ignore the parse error, whenever we can't transform the value text to specifed struct type,
// the parser will continue parsing the next struct field.
//
// The default value is false.
func WithIgnoreFieldParseError[T any](b bool) NewParserOption[T] {
	return func(cp *CsvParser[T]) {
		cp.ignoreFieldParseError = b
	}
}

// Stop the parsing process and close the data channel.
// You should call this method when task finished or aborted.
func (parser *CsvParser[T]) Close() {
	parser.closeOnce.Do(func() {
		close(parser.closeCh)
	})
}

// If parsing failed, return the parsing error
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
		if !sf.IsExported() { // ignore unexported fields
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
				optKVs := make(map[string]string)
				kvs := strings.Split(opts, ",")
				if len(kvs) == 0 {
					continue
				}
				for _, v := range kvs {
					optKey, optVal, _ := strings.Cut(strings.TrimSpace(v), "=")
					optKVs[optKey] = strings.TrimSpace(optVal)
				}
				// handle "required" option
				if _, ok := optKVs[FieldTagOpt_Required]; ok {
					header.required = strings.Contains(opts, FieldTagOpt_Required)
				}
				// handle "default" option, ignore if fieldType is not primitive type
				if val, ok := optKVs[FieldTagOpt_Default]; ok {
					typ := sf.Type
					if !parser.isPrimitiveType(typ) {
						return fmt.Errorf("only primitive type can have default value")
					}
					v, err := parsePrimitive(val, typ)
					if err != nil && !errors.Is(err, errEmptyValue) {
						return fmt.Errorf("bad default value: %s, err: %v", val, err)
					}
					header.defaultVal = &v
				}
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

func (parser *CsvParser[T]) isPrimitiveType(typ reflect.Type) bool {
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

func (parser *CsvParser[T]) isSupportedStructFieldType(typ reflect.Type) bool {
	return parser.isPrimitiveType(typ) ||
		(typ.Kind() == reflect.Slice && parser.isPrimitiveType(typ.Elem())) ||
		(typ.Kind() == reflect.Map && typ.Key().Kind() == reflect.String && parser.isPrimitiveType(typ.Elem()))

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
		return fmt.Errorf("some required headers not found in csv file header: %v", strings.Join(keys, ","))
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

// The next line number to be read, index starting from one, including the headers line.
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
		if err == io.EOF { // all records were parsed
			close(parser.dataChan)
			return
		}

		parser.lineCursor++
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
			var (
				fieldHeader *fieldHeader
				ok          bool
			)
			if fieldHeader, ok = parser.fieldHeaders[name]; !ok {
				continue // ignore uninteresting field
			}
			var (
				fieldType     = fieldHeader.fieldType
				primitiveType = fieldType
				fieldIdx      = fieldHeader.fieldIndex
				fieldVal      = val.Elem().Field(fieldIdx)
				val           reflect.Value
				err           error
			)
			// slice or map
			if fieldType.Kind() == reflect.Slice || fieldType.Kind() == reflect.Map {
				primitiveType = fieldType.Elem()
			}

			val, err = parsePrimitive(record[j], primitiveType)
			if err != nil {
				if errors.Is(err, errEmptyValue) {
					if fieldHeader.hasDefaultVal() {
						val = *fieldHeader.defaultVal
					}
				} else {
					if parser.ignoreFieldParseError {
						if fieldHeader.hasDefaultVal() {
							val = *fieldHeader.defaultVal
						} else {
							continue
						}
					} else {
						parser.err = err
						return
					}
				}
			}

			switch fieldType.Kind() {
			case reflect.Slice:
				fieldVal.Set(reflect.Append(fieldVal, val))
			case reflect.Map:
				if fieldVal.IsNil() {
					fieldVal.Set(reflect.MakeMap(fieldType))
				}
				fieldVal.SetMapIndex(reflect.ValueOf(fileHeader.mapKey), val)
			default:
				fieldVal.Set(val)
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

var (
	errEmptyValue = errors.New("empty value")
)

func parsePrimitive(txt string, fieldType reflect.Type) (val reflect.Value, err error) {
	var v any
	fieldKind := fieldType.Kind()
	switch fieldKind {
	case reflect.Bool:
		txt := strings.TrimSpace(txt)
		switch txt {
		case "true", "1":
			v, err = true, nil
		case "false", "0":
			v, err = false, nil
		case "":
			v, err = false, errEmptyValue
		default:
			v, err = nil, fmt.Errorf("unknown bool value: %s", txt)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, err = parseInt(strings.TrimSpace(txt), fieldKind)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, err = parseUint(strings.TrimSpace(txt), fieldKind)
	case reflect.Float32, reflect.Float64:
		v, err = parseFloat(strings.TrimSpace(txt), fieldKind)
	case reflect.String:
		if txt == "" {
			v, err = "", errEmptyValue
		} else {
			v, err = txt, nil
		}
	default:
		v, err = nil, fmt.Errorf("unsupported primitive field type kind, kind: %v", fieldKind.String())
	}
	if v == nil {
		return reflect.Value{}, err
	}
	return reflect.ValueOf(v).Convert(fieldType), err
}

func parseInt(txt string, kind reflect.Kind) (int64, error) {
	if txt == "" {
		return 0, errEmptyValue
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
		return 0, errEmptyValue
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
		return 0, errEmptyValue
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
