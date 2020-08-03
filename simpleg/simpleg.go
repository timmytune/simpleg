package simpleg

import (
	"errors"
	"fmt"
	"sync"

	kv "ytech.com.ng/projects/jists/keyvalue"
)

var (
	KV *kv.KV
)

type FieldType interface {
	GetOption() map[string]string
	Set(interface{}) []byte
	Get([]byte) interface{}
	Compare(string, []byte, []byte) (bool, error)
	CompareIndexed(typ string, a interface{}) (string, string, error)
}

type FieldTypeOptions struct {
	Name          string
	AllowIndexing bool
}

//FieldOptions saves the options reqiored for a field
type FieldOptions struct {
	Indexed   bool
	Validate  func(interface{}, *DB) (bool, interface{}, error)
	FieldType string
}

type ObjectTypeOptions struct {
	Name     string
	Fields   map[string]FieldOptions
	Set      func(interface{}, *DB) (map[string][]byte, []error)
	Validate func(interface{}, *DB) (interface{}, []error)
	Get      func(map[string][]byte, *DB) (interface{}, []error)
	New      func(*DB) interface{}
}

type Options struct {
	DataDirectory          string
	DBName                 string
	TruncateDB             bool
	DBDelimiter            string
	KVWriterGoroutineCount int
	KVWriterChannelLength  int
	SetterChannelLength    int
	SetterGoroutineCount   int
}

func DefaultOptions() Options {
	return Options{
		DataDirectory:          "/db/simpleg",
		DBName:                 "simpleg",
		TruncateDB:             true,
		DBDelimiter:            "^",
		KVWriterChannelLength:  500,
		KVWriterGoroutineCount: 100,
		SetterChannelLength:    500,
		SetterGoroutineCount:   200}
}

// DB is simpleg's main db
type DB struct {
	Options Options
	KV      *kv.KV
	FT      map[string]FieldType
	FTO     map[string]FieldTypeOptions
	OT      map[string]ObjectTypeOptions
	Setter  SetterFactory
	Getter  GetterFactory
	Lock    sync.Mutex
}

func (db *DB) Init(o Options) {
	db.Lock = sync.Mutex{}
	db.Options = o
	db.FT = make(map[string]FieldType)
	db.FTO = make(map[string]FieldTypeOptions)
	db.OT = make(map[string]ObjectTypeOptions)
	db.Setter = SetterFactory{}
	db.Getter = GetterFactory{}
	db.AddFieldType(FieldTypeOptions{Name: "bool", AllowIndexing: false}, &FieldTypeBool{})
	db.AddFieldType(FieldTypeOptions{Name: "string", AllowIndexing: true}, &FieldTypeString{})
	db.AddFieldType(FieldTypeOptions{Name: "int64", AllowIndexing: true}, &FieldTypeInt64{})
	db.AddFieldType(FieldTypeOptions{Name: "uint64", AllowIndexing: true}, &FieldTypeUint64{})

}
func (db *DB) Set(d ...interface{}) (s SetterRet) {
	defer func() {
		r := recover()
		if r != nil {
			fmt.Println("Recovered in Setter.Run ", r)
			s = SetterRet{}
			if s.Errors == nil {
				s.Errors = make([]error, 1)
			}
			switch x := r.(type) {
			case string:
				s.Errors = append(s.Errors, errors.New(x))
			case error:
				s.Errors = append(s.Errors, x)
			default:
				s.Errors = append(s.Errors, errors.New("Unknown error was thrown"))
			}
		}
	}()
	j := SetterJob{}
	j.Ins = d[0].(string)
	j.Data = d[1:]
	ch := make(chan SetterRet)
	j.Ret = ch
	db.Setter.Input <- j
	s = <-ch
	return

}

func (db *DB) Get(d ...interface{}) *GetterRet {
	return &Getter{DB: db}
}

func (db *DB) Query() Query {
	return Query{DB: db}
}

func (db *DB) Start() error {
	var err error
	kd := kv.GetDefaultKVOptions()
	kd.D = db.Options.DBDelimiter
	kd.WriteTransactionsChannelLength = db.Options.KVWriterChannelLength
	kd.WriterRoutines = db.Options.KVWriterGoroutineCount
	bd := kv.BadgerDefaultOptions(db.Options.DataDirectory)
	bd.Truncate = db.Options.TruncateDB
	db.KV, err = kv.Open(kd, bd)
	db.Setter.Start(db, db.Options.SetterGoroutineCount, db.Options.SetterChannelLength)
	return err
}

func (db *DB) Close() error {
	var err error
	err = db.KV.Close()
	return err
}

func (db *DB) AddFieldType(o FieldTypeOptions, f FieldType) error {
	var err error
	if f == nil {
		return errors.New("invalid fieldtype provided")
	}
	db.FT[o.Name] = f
	db.FTO[o.Name] = o
	return err
}

func (db *DB) AddObjectType(o ObjectTypeOptions) error {
	var err error
	if o.Name == "" {
		return errors.New("invalid ObjectTypeOption provided")
	}
	db.OT[o.Name] = o
	return err
}
