/*
 * Copyright 2021 Adedoyin Yinka and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package simpleg

import (
	"errors"
	"os"
	"runtime/debug"
	"sync"

	badger "github.com/dgraph-io/badger/v3"
	badgerOptions "github.com/dgraph-io/badger/v3/options"
	"github.com/rs/zerolog"
	kv "github.com/timmytune/simpleg/keyvalue"
)

var (
	//Log is the current Zerolog instance used by simpleg. It is initialised and you can use it in your own application too
	Log zerolog.Logger
)

//FieldType is the Interface that all fieldtypes in simpleg Implement.
type FieldType interface {
	//GetOption returns the options of type map[string]string for the FieldType. Only two options are currently provided
	//check file FieldTypeBool.go for implementation
	GetOption() map[string]string
	//Set accepts an interface of which's underlaying value is the valur type of this FiledType and returns []byte representation of the
	//value, If there is an error an error is returned instead
	//check file FieldTypeBool.go for implementation
	Set(interface{}) ([]byte, error)
	//Get accepts an []byte representation of the value and returns an interface of which's underlaying value is the valur type of this
	//FiledType, If there is an error an error is returned instead
	//check file FieldTypeBool.go for implementation
	Get([]byte) (interface{}, error)
	//Compare is used for field comparison in simpleg. It accepts a string and two []byte. The string represents the comparison instruction to //be executed, the first []byte representing the raw value from database and the second representing raw value of a parammeter provided in
	//query. it returns a boolean valued based on the comparison result and an error if there is any
	//check file FieldTypeBool.go for implementation
	Compare(string, []byte, []byte) (bool, error)
	//CompareIndexed accepts a string and an interface. It is used in comparison for indexed fields. The string is the instruction and the
	//interface is the parameter to be compared against. it returns a first string representation of the provided parameter, another string
	//representation of the instruction that simpleg understands listed below
	//"==" return all results that match parameter or which parameter is a prefix of
	//"+=" return all results that are greater than or equal to parameter
	//"+" return all results that are greater than parameter
	//"-=" return all results that are less than or equal to parameter
	//"-" return all results that are less than parameter
	//the final returned varible is an error which will be a valid error if the function encouters an error
	CompareIndexed(typ string, a interface{}) (string, string, error)
}

//AdvancedFieldType is the Interface that all AdvancedFieldtypes in simpleg Implement.
type AdvancedFieldType interface {
	//GetOption returns the options of type map[string]string for the FieldType. Only 1 option is currently provided
	//check file FieldTypeArray.go for implementation
	GetOption() map[string]string
	//New can be customised based on type, it is not used by simpleg core
	//check file FieldTypeArray.go for implementation
	New(db *DB, params ...interface{}) (interface{}, []error)
	//Set is responsible for saving information to DB and the implementation is left to the AdvancedFieldType. This is not called
	// internally by simpleg.
	//check file FieldTypeArray.go for implementation
	Set(db *DB, params ...interface{}) []error
	//Set is responsible for Getting data from DB and the implementation is left to the AdvancedFieldType. This is not called
	// internally by simpleg.
	//check file FieldTypeArray.go for implementation
	Get(txn *badger.Txn, db *DB, params ...interface{}) (interface{}, []error)
	//Compare is responsible for comparison of fields. This is called internally by simpleg if a field of this particular type is used in
	//a nodequery.
	//check file FieldTypeArray.go for implementation
	Compare(*badger.Txn, *DB, bool, string, []byte, []byte, string, string, interface{}) (bool, []error)
	//Delete is responsible for deleting fields. This is called internally by simpleg if a field of this particular type is used in
	//an object or a link that is to be deleted.
	//check file FieldTypeArray.go for implementation
	Delete(*badger.Txn, *DB, bool, string, []byte, []byte, string) []error
	//Close is responsible for doimg all type cleanups when DB is about to shotdown
	Close() error
}

type FieldTypeOptions struct {
	Name          string
	AllowIndexing bool
}

//FieldOptions saves the options reqiored for a field
type FieldOptions struct {
	Indexed          bool
	Validate         func(interface{}, *DB) (bool, interface{}, error)
	FieldType        string
	Advanced         bool
	FieldTypeOptions []interface{}
}

type ObjectTypeOptions struct {
	Name     string
	Fields   map[string]FieldOptions
	Set      func(interface{}, *DB) (map[KeyValueKey][]byte, []error)
	Validate func(interface{}, *DB) (interface{}, []error)
	Get      func(map[KeyValueKey][]byte, *DB) (interface{}, []error)
	New      func(*DB) interface{}
}

type LinkTypeOptions struct {
	Name     string
	Type     int
	From     string
	To       string
	Fields   map[string]FieldOptions
	Set      func(interface{}, *DB) (map[KeyValueKey][]byte, []error)
	Validate func(interface{}, *DB) (interface{}, []error)
	Get      func(map[KeyValueKey][]byte, *DB) (interface{}, []error)
	New      func(*DB) interface{}
}

type Options struct {
	DBName                 string
	DBDelimiter            string
	KVWriterGoroutineCount int
	KVWriterChannelLength  int
	SetterChannelLength    int
	SetterGoroutineCount   int
	GetterChannelLength    int
	GetterGoroutineCount   int
	LoggerFile             string
	BadgerOptions          badger.Options
}

func DefaultOptions() Options {
	//opts = opts.WithValueLogFileSize(16 << 20) // 16 MB value log file
	//opts = opts.WithMaxCacheSize(8 << 20)
	//opts = opts.WithMaxTableSize(8 << 20)
	ret := Options{
		DBName:                 "sg",
		DBDelimiter:            "^",
		KVWriterChannelLength:  500,
		KVWriterGoroutineCount: 50,
		SetterChannelLength:    500,
		SetterGoroutineCount:   25,
		GetterChannelLength:    500,
		GetterGoroutineCount:   50,
		LoggerFile:             "/simpleg.log",
		BadgerOptions:          badger.DefaultOptions("/data/simpleg")}

	ret.BadgerOptions.Compression = badgerOptions.None
	ret.BadgerOptions.DetectConflicts = false
	ret.BadgerOptions.IndexCacheSize = 512 << 20
	return ret
}

// DB :- is simpleg's main db
type DB struct {
	Options  Options
	KV       *kv.KV
	FT       map[string]FieldType
	FTO      map[string]FieldTypeOptions
	AFT      map[string]AdvancedFieldType
	OT       map[string]ObjectTypeOptions
	LT       map[string]LinkTypeOptions
	GF       map[string]func(*GetterFactory, *badger.Txn, *map[string]interface{}, *Query, []interface{}, *GetterRet)
	Setter   SetterFactory
	Getter   GetterFactory
	Shotdown bool
	sync.RWMutex
}

func (db *DB) Init(o Options) error {
	file, err := os.OpenFile(o.LoggerFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	Log = zerolog.New(file).With().Timestamp().Logger()
	//os.Stderr = file
	//log.SetOutput(file)
	Log.Info().Msg("Database initializing")
	kv.Log = &Log
	//zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	//db.Lock = sync.Mutex{}
	db.Options = o
	db.FT = make(map[string]FieldType)
	db.FTO = make(map[string]FieldTypeOptions)
	db.OT = make(map[string]ObjectTypeOptions)
	db.LT = make(map[string]LinkTypeOptions)
	db.AFT = make(map[string]AdvancedFieldType)
	db.GF = make(map[string]func(*GetterFactory, *badger.Txn, *map[string]interface{}, *Query, []interface{}, *GetterRet))
	db.Setter = SetterFactory{}
	db.Getter = GetterFactory{}
	db.AddFieldType(FieldTypeOptions{Name: "bool", AllowIndexing: false}, &FieldTypeBool{})
	db.AddFieldType(FieldTypeOptions{Name: "string", AllowIndexing: true}, &FieldTypeString{})
	db.AddFieldType(FieldTypeOptions{Name: "int64", AllowIndexing: true}, &FieldTypeInt64{})
	db.AddFieldType(FieldTypeOptions{Name: "uint64", AllowIndexing: true}, &FieldTypeUint64{})
	db.AddFieldType(FieldTypeOptions{Name: "date", AllowIndexing: true}, &FieldTypeDate{})
	db.AddAdvancedFieldType("array", &FieldTypeArray{})

	Log.Info().Msg("Database initialized")
	return nil
}

func (db *DB) Set(ins string, d ...interface{}) (s SetterRet) {
	defer func() {
		r := recover()
		if r != nil {
			Log.Error().Interface("recovered", r).Interface("stack", debug.Stack()).Msg("Recovered in DB.Set ")
			s = SetterRet{}
			if s.Errors == nil {
				s.Errors = make([]error, 0)
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
	if db.Shotdown {
		s.Errors = append(s.Errors, errors.New("Database shoting down..."))
		return
	}
	j := SetterJob{}
	j.Ins = ins
	j.Data = d
	ch := make(chan SetterRet)
	j.Ret = ch
	db.Setter.Input <- j
	s = <-ch
	return
}

func (db *DB) Delete(ins string, d ...interface{}) (s SetterRet) {
	defer func() {
		r := recover()
		if r != nil {
			Log.Error().Interface("recovered", r).Interface("stack", debug.Stack()).Msg("Recovered in DB.Set ")
			s = SetterRet{}
			if s.Errors == nil {
				s.Errors = make([]error, 0)
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
	s = db.Set(ins, d...)
	return
}

func (db *DB) Get(ins string, d ...interface{}) (ret GetterRet) {
	q := Query{DB: db}
	defer func() {
		r := recover()
		if r != nil {
			Log.Error().Interface("recovered", r).Interface("stack", debug.Stack()).Msg("Recovered in DB.Get ")
			if ret.Errors == nil {
				ret.Errors = make([]error, 0)
			}
			switch x := r.(type) {
			case string:
				ret.Errors = append(ret.Errors, errors.New(x))
			case error:
				ret.Errors = append(ret.Errors, x)
			default:
				ret.Errors = append(ret.Errors, errors.New("Unknown error was thrown"))
			}
		}
	}()
	if db.Shotdown {
		ret.Errors = append(ret.Errors, errors.New("Database shoting down..."))
		return
	}
	d1, ok := d[0].(string)
	if !ok {
		ret.Errors = append(ret.Errors, errors.New("Invalid argument provided in Get function"))
	}
	switch ins {
	case "object.single":
		n := NodeQuery{}
		n.Name("da").Object(d1).Q("ID", "==", d[1])
		q.Do("object", n)
		ret = q.Return("single", "da", 0)
	case "object.new":
		q.Do("object.new", d1)
		ret = q.Return("skip")
	case "link.new":
		q.Do("link.new", d1)
		ret = q.Return("skip")
	case "link.single":
		d2, ok := d[1].(string)
		if !ok {
			ret.Errors = append(ret.Errors, errors.New("Invalid argument provided in Get function"))
		}
		n := NodeQuery{}
		n.Name("da").Link(d1, d2).Q("FROM", "==", d[2]).Q("TO", "==", d[3])
		q.Do("link", n)
		ret = q.Return("single", "da", 0)
	default:
		ret.Errors = make([]error, 0)
		ret.Errors = append(ret.Errors, errors.New("Invalid Instruction"))
	}
	return
}

func (db *DB) Query() Query {
	return Query{DB: db}
}

func (db *DB) Start() error {
	Log.Info().Msg("Database starting...")
	var err error
	kd := kv.GetDefaultKVOptions()
	kd.D = db.Options.DBDelimiter
	kd.WriteTransactionsChannelLength = db.Options.KVWriterChannelLength
	kd.WriterRoutines = db.Options.KVWriterGoroutineCount
	db.KV, err = kv.Open(kd, db.Options.BadgerOptions)
	db.Setter.Start(db, db.Options.SetterGoroutineCount, db.Options.SetterChannelLength)
	db.Getter.Start(db, db.Options.GetterGoroutineCount, db.Options.GetterChannelLength)
	Log.Info().Msg("Database started")
	return err

}

func (db *DB) Close() error {
	Log.Print("Closing Database...")
	var err error
	db.Shotdown = true
	db.RLock()
	afts := db.AFT
	db.RUnlock()
	for _, v := range afts {
		v.Close()
	}
	db.Getter.Close()
	err = db.KV.Close()
	Log.Print("Database Closed")
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

func (db *DB) AddAdvancedFieldType(name string, f AdvancedFieldType) error {
	var err error
	if f == nil {
		return errors.New("invalid AdvancedFieldType provided")
	}
	db.AFT[name] = f
	return err
}

func (db *DB) AddGetterFunction(name string, f func(*GetterFactory, *badger.Txn, *map[string]interface{}, *Query, []interface{}, *GetterRet)) error {
	var err error
	if f == nil {
		return errors.New("invalid Getter function provided")
	}
	db.GF[name] = f
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

func (db *DB) AddLinkType(l LinkTypeOptions) error {
	var err error
	if l.Name == "" {
		return errors.New("invalid LinkTypeOption provided")
	}
	db.LT[l.Name] = l
	return err
}

func GetNewDB() *DB {
	return &DB{}
}
