package csv_parser

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

const (
	FieldTagKey          = "csv"
	FieldTagOpt_Omit     = "-"        // 如果tag名为此字符，则此字段不参与解析
	FieldTagOpt_Required = "required" // 如果tag选项中有此字段，则对应csv文件表头必须有此字段，否则报错
)

// 记录目标结构体中定义的header信息
type fieldHeader struct {
	name       string       // 表头字段名
	fieldIndex int          // 字段下标
	fieldType  reflect.Type // 字段反射类型
	required   bool         // 是否必填字段
}

func (fh *fieldHeader) String() string {
	return fmt.Sprintf("{name: %s, fieldIndex: %d, fieldType: %v, required: %v}", fh.name, fh.fieldIndex, fh.fieldType, fh.required)
}

// 记录csv文件的header信息
type fileHeader struct {
	name string
}

type CsvParser[T any] struct {
	err                error
	reader             *csv.Reader             // csv的配置由调用方确定，如分隔符、换行符
	fileHeaders        []fileHeader            // 文件头及出现的列号
	fieldHeaders       map[string]*fieldHeader // 结构体T所定义的表头字段
	totalParsedRecords int                     // 记录已经解析的记录数，不包含表头行
	dataChan           chan *DataWrapper[T]    // 将解析的行数据记录在此通道中
	targetStructType   reflect.Type            // 想要解析为的目标结构体类型
	doParseOnce        sync.Once               // 一个parser只允许有一个解析goroutine
}

// 每行解析出的记录和错误信息，如果解析出错，则err != nil
type DataWrapper[T any] struct {
	Data *T
	Err  error
}

// 创建一个CsvParser，而后应当从DataChan方法中获取逐行解析的记录，类型T必须是一个struct类型，不允许是指向struct的指针。
// reader指向一个带有表头的csv文件，表头字段应当与T定义的表头在名称上对应，但是二者不必保持顺序上的对应。
// 如果csv文件中存在未在T中定义的表头字段，则在解析时忽略文件中定义的此字段信息。
// T中支持解析的字段类型有：bool,int,int8,int16,int32,int64,uint,uint8,uint16,uint32,uint64,float32,float64,string,
// 以及它们的指针类型，对于bool类型，合法的值为0,1,true,false，其中0，false表示false; 1，true表示true.
func NewCsvParser[T any](reader *csv.Reader) (parser *CsvParser[T], err error) {
	if reader == nil {
		return nil, errors.New("csv reader is nil")
	}
	parser = &CsvParser[T]{
		reader:           reader,
		fieldHeaders:     make(map[string]*fieldHeader),
		dataChan:         make(chan *DataWrapper[T]),
		targetStructType: reflect.TypeOf(new(T)).Elem(),
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

// 返回所有从目标结构体中得到的文件头字段，此函数通常用于排查问题
func (parser *CsvParser[T]) FieldHeaders() []string {
	var hds []string
	for _, v := range parser.fieldHeaders {
		hds = append(hds, v.String())
	}
	return hds
}

// 返回从csv文件中解析到的文件头字段，此函数通常用于排查问题
func (parser *CsvParser[T]) FileHeaders() []string {
	var hds []string
	for _, v := range parser.fileHeaders {
		hds = append(hds, v.name)
	}
	return hds
}

// 通过反射，从结构体中得到定义的文件头
// 如果没有使用csv tag定义表头名，则默认将字段名首字母小写作为表头名
func (parser *CsvParser[T]) getStructHeaders() (err error) {
	t := parser.targetStructType
	if t.Kind() != reflect.Struct {
		err = fmt.Errorf("type of T must be a struct, but we got %s", t.Kind().String())
		return err
	}

	headerNameSet := make(map[string]struct{}) // 用作校验是否存在相同的header名字
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
				if name == FieldTagOpt_Omit { // 忽略此字段
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
			// 默认表头名为字段名首字母小写
			if header.name == "" {
				header.name = sf.Name
				b := []byte(header.name)
				if 'A' <= b[0] && b[0] <= 'Z' { // 将首字母小写
					b[0] = (b[0] + 32)
				}
			}
			parser.fieldHeaders[header.name] = header
		}
	}
	if len(parser.fieldHeaders) == 0 {
		return errors.New("no struct header found")
	}
	return nil
}

// 支持 string, int, uint, float, bool 以及它们的指针类型
func (parser *CsvParser[T]) isSupportedStructFieldType(typ reflect.Type) bool {
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

// 读取csv文件中的header，表头字段不允许有重复
func (parser *CsvParser[T]) getFileHeaders() error {
	var err error
	record, err := parser.reader.Read() // 第一行是headers
	if err != nil {
		return err
	}
	for i := range record {
		parser.fileHeaders = append(parser.fileHeaders, fileHeader{name: strings.TrimSpace(record[i])})
	}
	// 校验文件中解析的头部是否重复
	fileHeaderSet := make(map[fileHeader]struct{})
	for i := range parser.fileHeaders {
		if _, ok := fileHeaderSet[parser.fileHeaders[i]]; ok {
			return fmt.Errorf("duplicate csv file header: %s", parser.fileHeaders[i].name)
		} else {
			fileHeaderSet[parser.fileHeaders[i]] = struct{}{}
		}
	}
	return nil
}

func (parser *CsvParser[T]) validateHeaders() error {
	// 匹配required选项
	requiredSet := map[string]struct{}{}
	for _, v := range parser.fieldHeaders {
		if v.required {
			requiredSet[v.name] = struct{}{}
		}
	}

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

	return nil
}

func (parser *CsvParser[T]) GetTotalParsedRecords() int {
	return parser.totalParsedRecords
}

// 从channel中不断获取解析的每行数据，可以用于多线程中
// 如果解析遇到错误，则返回的DataWrapper的Err不为nil，此后解析终止，channel关闭
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

	// 解析出错则发送一个错误，关闭channel
	defer func() {
		if parser.err != nil {
			parser.dataChan <- &DataWrapper[T]{Err: parser.err}
			close(parser.dataChan)
			return
		}
	}()

	for {
		record, err := parser.reader.Read()
		if err == io.EOF { // 成功解析完，则关闭通道
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
			if fieldHeader, ok := parser.fieldHeaders[fileHeader.name]; !ok {
				continue // 文件中的多余字段被忽略
			} else {
				fieldIdx := fieldHeader.fieldIndex
				fieldType := fieldHeader.fieldType
				fieldVal := val.Elem().Field(fieldIdx)
				if fieldType.Kind() == reflect.Pointer {
					fieldType = fieldType.Elem()
					fieldVal = reflect.New(fieldType)
					val.Elem().Field(fieldIdx).Set(fieldVal)
					fieldVal = fieldVal.Elem()
				}
				switch fieldType.Kind() {
				case reflect.Bool:
					txt := strings.TrimSpace(record[j])
					var b bool
					if txt == "true" || txt == "1" {
						b = true
					} else if txt != "false" && txt != "0" {
						parser.err = fmt.Errorf("unknown bool value: %s", txt)
						return
					}
					fieldVal.SetBool(b)
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					num, err := parseInt(strings.TrimSpace(record[j]), fieldType.Kind())
					if err != nil {
						parser.err = err
						return
					}
					fieldVal.SetInt(num)
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					num, err := parseUint(strings.TrimSpace(record[j]), fieldType.Kind())
					if err != nil {
						parser.err = err
						return
					}
					fieldVal.SetUint(num)
				case reflect.Float32, reflect.Float64:
					num, err := parseFloat(strings.TrimSpace(record[j]), fieldType.Kind())
					if err != nil {
						parser.err = err
						return
					}
					fieldVal.SetFloat(num)
				case reflect.String:
					fieldVal.SetString(record[j])
				default:
					parser.err = fmt.Errorf("unsupported field type kind, name: %v, kind: %v",
						fieldHeader.name, fieldType.Kind().String())
					return
				}
			}
		}

		parser.totalParsedRecords++

		select {
		case <-ctx.Done():
			parser.err = ctx.Err()
			return
		case parser.dataChan <- &DataWrapper[T]{Data: val.Interface().(*T), Err: nil}:
		}
	}
}

func parseInt(txt string, kind reflect.Kind) (int64, error) {
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
