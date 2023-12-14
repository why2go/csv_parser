package csv_reader

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unsafe"
)

// todo: 可以设置chan的大小

// type Demo struct {
// 	F1 string `csv:"f1,required"`
// 	F2 int8   `csv:"-"`
// }

const (
	FieldTag = "csv"

	FieldTag_Omit        = "-"        // 如果tag名为此字符，则此字段不参与解析
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
	totalParsedRecords int                     // 记录已经解析的记录数
	dataChan           chan *DataWrapper[T]    // 将解析的行数据记录在此通道中
	targetStructType   reflect.Type            // 想要解析为的目标结构体类型
}

// 每行解析出的记录和错误信息，如果解析出错，则err != nil
type DataWrapper[T any] struct {
	Record *T
	Err    error
}

// 创建一个CsvParser，类型T必须是一个struct类型，不允许是指向struct的指针。
// reader指向一个带有表头的csv文件，表头字段应当与T定义的表头在名称上对应，但是二者不必保持顺序上的对应。
// 如果csv文件中存在未在T中定义的表头，则在解析时忽略此字段信息。
func NewCsvParser[T any](reader *csv.Reader) (parser *CsvParser[T], err error) {
	if reader == nil {
		return nil, errors.New("csv reader is nil")
	}
	parser.targetStructType = reflect.TypeOf(new(T)).Elem()
	parser = &CsvParser[T]{
		reader:       reader,
		fieldHeaders: make(map[string]*fieldHeader),
	}
	err = parser.getStructHeaders()
	if err != nil {
		parser.err = err
		return nil, err
	}

	err = parser.getFileHeaders()
	if err != nil {
		parser.err = err
		return nil, err
	}

	err = parser.validateHeaders()
	if err != nil {
		parser.err = err
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
			if tag, ok := sf.Tag.Lookup(FieldTag); ok {
				name, opts, _ := strings.Cut(tag, ",")
				if name == FieldTag_Omit { // 忽略此字段
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

func (parser *CsvParser[T]) DataChan(ctx context.Context) <-chan *DataWrapper[T] {
	if parser.dataChan == nil {
		parser.dataChan = make(chan *DataWrapper[T])
	}
	go func() {
		defer func() { // 解析出错则发送一个错误，关闭channel
			if parser.err != nil {
				parser.dataChan <- &DataWrapper[T]{Err: parser.err}
				close(parser.dataChan)
				return
			}
		}()

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
		// log.Debug().Msgf("target type: %s, %s", target.Type(), target.Kind())
		for j := range record {
			fileHeader := parser.fileHeaders[j]
			if fieldHeader, ok := parser.fieldHeaders[fileHeader.name]; !ok {
				continue // 文件中的多余字段被忽略
			} else {
				fieldIdx := fieldHeader.fieldIndex
				if fieldHeader.fieldType.Kind() == reflect.Pointer {
					fieldVal := reflect.New(fieldHeader.fieldType)
					val.Elem().SetPointer(fieldVal.Interface().(unsafe.Pointer))
				}
				switch fieldHeader.fieldType.Kind() {
				case reflect.Bool:
					txt := strings.TrimSpace(record[j])
					var b bool
					if txt == "true" || txt == "1" {
						b = true
					} else if txt != "false" && txt != "0" {

					}
					// val.Elem().Field(fieldIdx).
					val.Elem().Field(fieldIdx).SetBool(b)
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				case reflect.Float32, reflect.Float64:
				case reflect.String:
					val.Elem().Field(fieldIdx).SetString(record[j])
				default:
				}
				val.Elem().Field(fieldIdx).SetString(strings.TrimSpace(record[j])) // 这里对每个字段做了trim space
			}
		}
		// log.Debug().Msgf("reuse: %t", parser.batchItems[i] == target.Interface().(*T))
		// items = append(items, target.Interface().(*T))
	}()
	return parser.dataChan
}
