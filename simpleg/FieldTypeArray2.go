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
	"bytes"
	"errors"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v3"
)

type ArrayOptions2 struct {
	Fields map[string]string
	New    func() interface{}
	Get    func(map[string][]byte, *DB) (interface{}, error)
	Set    func(interface{}, *DB) (map[string][]byte, error)
}

type FieldTypeArrayValue2 struct {
	IsObj    bool
	TypeID   uint64
	TypeIDTO uint64
	Type     string
	Field    string
	db       *DB
	Index    []string
	values   map[string]interface{}
}

func (f *FieldTypeArrayValue2) Set(v interface{}, index string) (string, error) {
	if f.values == nil {
		return "", errors.New("this field is not yet setup")
	}
	if index == "" {
		currentUnixTime := time.Now().UnixNano()
		index = strconv.FormatInt(currentUnixTime, 10)
		f.values[index] = v
		f.Index = append(f.Index, index)
	} else {
		_, ok := f.values[index]
		if !ok {
			f.Index = append(f.Index, index)
		}
		f.values[index] = v
	}

	return index, nil
}

func (f *FieldTypeArrayValue2) Delete(index string, field string) (errs []error) {
	defer func() {
		r := recover()
		if r != nil {
			switch x := r.(type) {
			case string:
				errs = append(errs, errors.New(x))
			case error:
				errs = append(errs, x)
			default:
				errs = append(errs, errors.New("unknown error was thrown"))
			}
			return
		}
	}()
	errs = make([]error, 0)
	if f.Type == "" {
		errs = append(errs, errors.New("this field has not yet been initialized"))
		return errs
	}

	if f.db.Shotdown {
		errs = append(errs, errors.New("database closing..."))
		return errs
	}

	txn := f.db.KV.DB.NewTransaction(true)
	defer txn.Discard()
	if index == "" {
		f.db.RLock()
		idRaw, err := f.db.FT["uint64"].Set(f.TypeID)
		if err != nil {
			errs = append(errs, err)
		}
		idToRaw, err := f.db.FT["uint64"].Set(f.TypeIDTO)
		if err != nil {
			errs = append(errs, err)
		}
		es := f.db.AFT["array2"].Delete(txn, f.db, f.IsObj, f.Type, idRaw, idToRaw, f.Field)
		f.db.RUnlock()
		if len(es) > 0 {
			errs = append(errs, es...)
		}
	} else if field != "" {
		f.db.RLock()
		idRaw, err := f.db.FT["uint64"].Set(f.TypeID)
		f.db.RUnlock()
		if err != nil {
			errs = append(errs, err)
		}
		if f.IsObj {
			err := txn.Delete([]byte(f.db.Options.DBName + f.db.KV.D + "a" + f.db.KV.D + "o" + f.db.KV.D + f.Type + f.db.KV.D + string(idRaw) + f.db.KV.D + f.Field + f.db.KV.D + index + f.db.KV.D + field))
			if err != nil {
				errs = append(errs, err)
			}
		} else {
			f.db.RLock()
			idToRaw, err := f.db.FT["uint64"].Set(f.TypeIDTO)
			if err != nil {
				errs = append(errs, err)
			}
			f.db.RUnlock()
			err = txn.Delete([]byte(f.db.Options.DBName + f.db.KV.D + "a" + f.db.KV.D + "l" + f.db.KV.D + f.Type + f.db.KV.D + string(idRaw) + "-" + string(idToRaw) + f.db.KV.D + f.Field + f.db.KV.D + index + f.db.KV.D + field))
			if err != nil {
				errs = append(errs, err)
			}
		}
	} else if field == "" {
		f.db.RLock()
		idRaw, _ := f.db.FT["uint64"].Set(f.TypeID)
		f.db.RUnlock()
		opt := badger.DefaultIteratorOptions
		if f.IsObj {
			opt.Prefix = []byte(f.db.Options.DBName + f.db.KV.D + "a" + f.db.KV.D + "o" + f.db.KV.D + f.Type + f.db.KV.D + string(idRaw) + f.db.KV.D + f.Field + f.db.KV.D + index)
		} else {
			f.db.RLock()
			idToRaw, err := f.db.FT["uint64"].Set(f.TypeIDTO)
			if err != nil {
				errs = append(errs, err)
			}
			f.db.RUnlock()
			opt.Prefix = []byte(f.db.Options.DBName + f.db.KV.D + "a" + f.db.KV.D + "l" + f.db.KV.D + f.Type + f.db.KV.D + string(idRaw) + "-" + string(idToRaw) + f.db.KV.D + f.Field + f.db.KV.D + index)
		}
		opt.PrefetchSize = 10
		opt.PrefetchValues = false
		iterator := txn.NewIterator(opt)
		iterator.Seek(opt.Prefix)
		for iterator.ValidForPrefix(opt.Prefix) {
			item := iterator.Item()
			k := item.KeyCopy(nil)
			err := txn.Delete(k)
			if err != nil {
				errs = append(errs, err)
			}
			iterator.Next()
		}
		iterator.Close()
	}
	err := txn.Commit()
	if err != nil {
		errs = append(errs, err)
	}
	return errs
}

func (f *FieldTypeArrayValue2) New() (v interface{}, errs []error) {
	defer func() {
		r := recover()
		if r != nil {
			switch x := r.(type) {
			case string:
				errs = append(errs, errors.New(x))
			case error:
				errs = append(errs, x)
			default:
				errs = append(errs, errors.New("Unknown error was thrown"))
			}
			return
		}
	}()
	if f.db.Shotdown {
		errs = append(errs, errors.New("Database closing..."))
		return
	}
	t := 1
	if !f.IsObj {
		t = 2
	}
	v, errs = f.db.AFT["array2"].New(f.db, t, f.Type, f.Field, f.TypeID, f.TypeIDTO)
	return
}

func (f *FieldTypeArrayValue2) Get(index string) interface{} {

	if index == "" {
		return nil
	}
	r, ok := f.values[index]
	if !ok {
		return nil
	}
	return r

}

func (f *FieldTypeArrayValue2) GetList() []string {
	sort.Slice(f.Index, func(i, j int) bool { return f.Index[i] < f.Index[j] })
	return f.Index

}

func (f *FieldTypeArrayValue2) Clear() {
	f.values = make(map[string]interface{})
	f.Index = make([]string, 0)
}

func (f *FieldTypeArrayValue2) FromDB(ins string, params ...interface{}) (errs []error) {
	defer func() {
		r := recover()
		if r != nil {
			switch x := r.(type) {
			case string:
				errs = append(errs, errors.New(x))
			case error:
				errs = append(errs, x)
			default:
				errs = append(errs, errors.New("Unknown error was thrown"))
			}
			return
		}
	}()
	if f.db.Shotdown {
		errs = append(errs, errors.New("database closing..."))
		return
	}
	errs = make([]error, 0)
	switch ins {
	case "single":
		txn := f.db.KV.DB.NewTransaction(false)
		defer txn.Discard()
		var obj interface{}
		if len(params) == 0 {
			errs = append(errs, errors.New("You need to provide the index of the array value to load"))
			return
		}
		f.db.RLock()
		aft := f.db.AFT["array2"]
		obj, errs = aft.Get(txn, f.db, f.IsObj, f.Type, f.Field, ins, params[0], f.TypeID, f.TypeIDTO)
		f.db.RUnlock()
		if len(errs) > 0 {
			return
		}
		index, _ := params[0].(string)
		_, ok := f.values[index]
		if !ok {
			f.Index = append(f.Index, index)
		}
		f.values[index] = obj
		//sort.Slice(f.Index, func(i, j int) bool { return f.Index[i] < f.Index[j] })
	case "last":
		txn := f.db.KV.DB.NewTransaction(false)
		defer txn.Discard()
		var obj interface{}
		if len(params) == 0 {
			errs = append(errs, errors.New("you need to provide the number of array values to load"))
			return
		}
		f.db.RLock()
		obj, errs = f.db.AFT["array2"].Get(txn, f.db, f.IsObj, f.Type, f.Field, ins, f.TypeID, f.TypeIDTO, params[0])
		f.db.RUnlock()
		if obj == nil {
			return
		}
		for i, v := range obj.(map[string]interface{}) {
			if _, ok := f.values[i]; !ok {
				f.Index = append(f.Index, i)
			}
			f.values[i] = v
		}
	case "list":
		txn := f.db.KV.DB.NewTransaction(false)
		defer txn.Discard()
		var obj interface{}
		if len(params) < 2 {
			errs = append(errs, errors.New("You need to provide the limit and skip of array values to load"))
			return
		}
		f.db.RLock()
		obj, errs = f.db.AFT["array2"].Get(txn, f.db, f.IsObj, f.Type, f.Field, ins, f.TypeID, f.TypeIDTO, params[0], params[1])
		f.db.RUnlock()
		if obj == nil {
			return
		}
		for i, v := range obj.(map[string]interface{}) {
			if _, ok := f.values[i]; !ok {
				f.Index = append(f.Index, i)
			}
			f.values[i] = v
		}
	}
	return

}

func (f *FieldTypeArrayValue2) Pop() (interface{}, string) {
	if len(f.values) >= 1 {
		var k string
		var v interface{}
		k, f.Index = f.Index[len(f.Index)-1], f.Index[:len(f.Index)-1]
		v = f.values[k]
		delete(f.values, k)
		return v, k
	}
	return nil, ""
}

func (f *FieldTypeArrayValue2) Save() (errs []error) {
	defer func() {
		r := recover()
		if r != nil {
			switch x := r.(type) {
			case string:
				errs = append(errs, errors.New(x))
			case error:
				errs = append(errs, x)
			default:
				errs = append(errs, errors.New("Unknown error was thrown"))
			}
			return
		}
	}()
	if f.db.Shotdown {
		errs = append(errs, errors.New("Database closing..."))
		return
	}
	f.db.RLock()
	errs = f.db.AFT["array2"].Set(f.db, *f)
	f.db.RUnlock()
	return
}

type FieldTypeArray2 struct {
}

func (f *FieldTypeArray2) GetOption() map[string]string {
	m := make(map[string]string)
	m["Name"] = "array2"
	return m
}

func (f *FieldTypeArray2) New(db *DB, params ...interface{}) (v interface{}, errs []error) {
	defer func() {
		r := recover()
		if r != nil {
			switch x := r.(type) {
			case string:
				errs = append(errs, errors.New(x))
			case error:
				errs = append(errs, x)
			default:
				errs = append(errs, errors.New("Unknown error was thrown"))
			}
			return
		}
	}()
	if db.Shotdown {
		errs := make([]error, 0)
		errs = append(errs, errors.New("database closing..."))
		return nil, errs
	}
	errs = make([]error, 0)
	isObj, ok := params[0].(int)
	if !ok {
		errs = append(errs, errors.New("first parameter not an int"))
		return nil, errs
	}

	typ, ok := params[1].(string)
	if !ok {
		errs = append(errs, errors.New("Second parameter not a string"))
		return nil, errs
	}

	field, ok := params[2].(string)
	if !ok {
		errs = append(errs, errors.New("Third parameter not a string"))
		return nil, errs
	}
	var ret interface{}
	var ao ArrayOptions2
	if isObj == 1 {
		db.RLock()
		ao, ok = db.OT[typ].Fields[field].FieldTypeOptions[0].(ArrayOptions2)
		db.RUnlock()
		if !ok {
			errs = append(errs, errors.New("Field "+field+" Not found for Object "+typ))
			return nil, errs
		}
		ret = ao.New()
	} else if isObj == 2 {
		db.RLock()
		ao, ok = db.LT[typ].Fields[field].FieldTypeOptions[0].(ArrayOptions2)
		db.RUnlock()
		if !ok {
			errs = append(errs, errors.New("Field "+field+" Not found for Object "+typ))
			return nil, errs
		}
		ret = ao.New()
	} else if isObj == 3 {
		if len(params) < 5 {
			errs = append(errs, errors.New("insufficient parameters"))
			return nil, errs
		}
		isObject, ok := params[3].(bool)
		if !ok {
			errs = append(errs, errors.New("Fourth parameter not bool"))
			return nil, errs
		}
		id, ok := params[4].(uint64)
		if !ok {
			errs = append(errs, errors.New("Fifth parameter not uint64"))
			return nil, errs
		}
		idTO := uint64(0)
		if len(params) >= 6 {
			idTO, ok = params[5].(uint64)
			if !ok {
				errs = append(errs, errors.New("Sixth parameter not uint64"))
				return nil, errs
			}
		}

		f := FieldTypeArrayValue2{}
		f.values = make(map[string]interface{})
		f.Index = make([]string, 0)
		f.IsObj = isObject
		f.Type = typ
		f.Field = field
		f.TypeID = id
		f.TypeIDTO = idTO
		f.db = db
		ret = f
	}
	return ret, errs
}

func (f *FieldTypeArray2) Set(db *DB, params ...interface{}) (errs []error) {
	defer func() {
		r := recover()
		if r != nil {
			switch x := r.(type) {
			case string:
				errs = append(errs, errors.New(x))
			case error:
				errs = append(errs, x)
			default:
				errs = append(errs, errors.New("unknown error was thrown"))
			}
			return
		}
	}()
	if db.Shotdown {
		errs = append(errs, errors.New("database closing..."))
		return errs
	}
	array, ok := params[0].(FieldTypeArrayValue2)
	if !ok {
		errs = append(errs, errors.New("FieldArray.Set: Provided data is not of type FieldTypeArrayValue2"))
		return errs
	}
	var ao ArrayOptions2
	t := "o"
	if array.IsObj {
		db.RLock()
		ao, ok = db.OT[array.Type].Fields[array.Field].FieldTypeOptions[0].(ArrayOptions2)
		db.RUnlock()
	} else {
		t = "l"
		db.RLock()
		ao, ok = db.LT[array.Type].Fields[array.Field].FieldTypeOptions[0].(ArrayOptions2)
		db.RUnlock()
	}
	if !ok {
		errs = append(errs, errors.New("FieldArray2.Set: Field "+array.Field+" Not found for Object/Link "+array.Type+" "+array.Field+" "+t))
		return errs
	}
	var idraw string
	if array.IsObj {
		idr, _ := db.FT["uint64"].Set(array.TypeID)
		idraw = string(idr)
	} else {
		idr, _ := db.FT["uint64"].Set(array.TypeID)
		idr2, _ := db.FT["uint64"].Set(array.TypeIDTO)
		idraw = string(idr) + "-" + string(idr2)
	}

	for i, val := range array.values {
		v, err := ao.Set(val, db)
		if err != nil {
			errs = append(errs, err)
		}

		for ii, value := range v {
			db.KV.Writer2.Write(value, db.Options.DBName, "a", t, array.Type, idraw, array.Field, i, ii)
		}
	}

	return errs
}

func (f *FieldTypeArray2) Get(txn *badger.Txn, db *DB, params ...interface{}) (v interface{}, errs []error) {
	defer func() {
		r := recover()
		if r != nil {
			switch x := r.(type) {
			case string:
				errs = append(errs, errors.New(x))
			case error:
				errs = append(errs, x)
			default:
				errs = append(errs, errors.New("Unknown error was thrown"))
			}
			return
		}
	}()
	errs = make([]error, 0)
	if db.Shotdown {
		errs = append(errs, errors.New("database closing..."))
		return nil, errs
	}
	isObj, ok := params[0].(bool)
	if !ok {
		errs = append(errs, errors.New("FieldArray.Get: First parameter not a bool"))
		return nil, errs
	}

	typ, ok := params[1].(string)
	if !ok {
		errs = append(errs, errors.New("FieldArray.Get: Second parameter not a string"))
		return nil, errs
	}

	field, ok := params[2].(string)
	if !ok {
		errs = append(errs, errors.New("FieldArray.Get: Third parameter not a string"))
		return nil, errs
	}
	var ao ArrayOptions2
	t := "o"
	if isObj {
		db.RLock()
		ao, ok = db.OT[typ].Fields[field].FieldTypeOptions[0].(ArrayOptions2)
		db.RUnlock()
	} else {
		t = "l"
		db.RLock()
		ao, ok = db.LT[typ].Fields[field].FieldTypeOptions[0].(ArrayOptions2)
		db.RUnlock()
	}
	if !ok {
		errs = append(errs, errors.New("FieldArray.Get: Field "+field+" Not found for Object/Link "+typ))
		return nil, errs
	}

	ins, ok := params[3].(string)
	if !ok {
		errs = append(errs, errors.New("FieldArray.Get: Fourth parameter not a string"))
		return nil, errs
	}

	switch ins {
	case "single":
		index := params[4].(string)
		id, err := db.FT["uint64"].Set(params[5])
		if err != nil {
			errs = append(errs, err)
			return nil, errs
		}
		idTO, err := db.FT["uint64"].Set(params[6])
		if err != nil {
			errs = append(errs, err)
			return nil, errs
		}
		opt := badger.DefaultIteratorOptions
		if isObj {
			opt.Prefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(id) + db.KV.D + field + db.KV.D + index)
		} else {
			opt.Prefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(id) + "-" + string(idTO) + db.KV.D + field + db.KV.D + index)
		}
		opt.PrefetchSize = 5
		opt.PrefetchValues = true
		iterator := txn.NewIterator(opt)
		defer iterator.Close()
		iterator.Seek(opt.Prefix)
		data := make(map[string][]byte)
		var k []byte
		var kArray [][]byte
		var v []byte
		for iterator.ValidForPrefix(opt.Prefix) {
			item := iterator.Item()
			k = item.KeyCopy(k)
			kArray = bytes.Split(k, []byte(db.KV.D))
			v, err = item.ValueCopy(nil)
			if err != nil {
				Log.Error().Interface("error", err).Interface("stack", debug.Stack()).Str("key", string(k)).Msg("Getting value for key in Badger threw error")
			} else {
				data[string(kArray[7])] = v
			}
			iterator.Next()
		}
		ret, err := ao.Get(data, db)
		if err != nil {
			errs = append(errs, err)
			return nil, errs
		}
		return ret, errs
	case "list":
		objectID, err := db.FT["uint64"].Set(params[4])
		if err != nil {
			errs = append(errs, err)
			return nil, errs
		}
		objectIDTO, err := db.FT["uint64"].Set(params[5])
		if err != nil {
			errs = append(errs, err)
			return nil, errs
		}
		skip, ok := params[6].(int)
		if !ok {
			errs = append(errs, errors.New("Sixth parameter is not an int"))
			return nil, errs
		}
		limit, ok := params[7].(int)
		if !ok {
			errs = append(errs, errors.New("Seventh parameter is not an int"))
			return nil, errs
		}
		opt := badger.DefaultIteratorOptions
		if isObj {
			opt.Prefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(objectID) + db.KV.D + field)
		} else {
			opt.Prefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(objectID) + "-" + string(objectIDTO) + db.KV.D + field)
		}
		opt.PrefetchSize = 5
		opt.PrefetchValues = true
		iterator := txn.NewIterator(opt)
		defer iterator.Close()
		iterator.Seek(opt.Prefix)
		data := make(map[string]map[string][]byte)
		var k []byte
		var kArray [][]byte
		var v []byte
		currentID := ""
		skipped := 0
		for iterator.ValidForPrefix(opt.Prefix) {
			item := iterator.Item()
			k = item.KeyCopy(k)
			kArray = bytes.Split(k, []byte(db.KV.D))
			var idd string
			v, err = item.ValueCopy(nil)
			if err != nil {
				Log.Error().Interface("error", err).Interface("stack", debug.Stack()).Str("key", string(k)).Msg("Getting value for key in Badger threw error")
			} else {
				idd = string(kArray[6])
				if skipped >= skip && currentID != idd {
					_, ok := data[idd]
					if !ok {
						data[idd] = make(map[string][]byte)
					}
					data[idd][string(kArray[7])] = v
				} else {
					if currentID != idd {
						skipped = skipped + 1
						currentID = idd
					}
				}
			}
			if len(data) > limit {
				delete(data, idd)
				break
			}
			iterator.Next()
		}
		re := make(map[string]interface{})
		for i, v := range data {
			r, err := ao.Get(v, db)
			if err == nil {
				re[i] = r
			} else {
				errs = append(errs, err)
			}
		}

		return re, errs
	case "last":
		objID, err := db.FT["uint64"].Set(params[4])
		if err != nil {
			errs = append(errs, err)
			return nil, errs
		}
		objIDTO, err := db.FT["uint64"].Set(params[5])
		if err != nil {
			errs = append(errs, err)
			return nil, errs
		}
		le, ok := params[6].(int)
		if !ok {
			errs = append(errs, errors.New("fifth parameter is not int"))
			return nil, errs
		}
		opt := badger.DefaultIteratorOptions
		var originalPrefix []byte
		if isObj {
			originalPrefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(objID) + db.KV.D + field)
		} else {
			originalPrefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(objID) + "-" + string(objIDTO) + db.KV.D + field)
		}

		opt.Prefix = append(originalPrefix, 0xFF)

		opt.PrefetchSize = 10
		opt.PrefetchValues = true
		opt.Reverse = true
		iterator := txn.NewIterator(opt)
		defer iterator.Close()
		iterator.Seek(opt.Prefix)
		data := make(map[string]map[string][]byte)
		var k []byte
		var kArray [][]byte
		var v []byte
		run := true
		for run {
			item := iterator.Item()
			k = item.KeyCopy(k)
			if !bytes.HasPrefix(k, originalPrefix) {
				break
			}
			kArray = bytes.Split(k, []byte(db.KV.D))
			v, err = item.ValueCopy(nil)
			var idd string
			if err != nil {
				Log.Error().Interface("error", err).Interface("stack", debug.Stack()).Str("key", string(k)).Msg("getting value for key in badger threw error")
			} else {
				idd = string(kArray[6])
				_, ok := data[idd]
				if !ok {
					data[idd] = make(map[string][]byte)
				}
				data[idd][string(kArray[7])] = v
			}
			if len(data) > le {
				delete(data, idd)
				break
			}
			iterator.Next()
		}
		re := make(map[string]interface{})
		for i, v := range data {
			r, err := ao.Get(v, db)
			if err == nil {
				re[i] = r
			} else {
				errs = append(errs, err)
			}
		}

		return re, errs
	default:
		errs = append(errs, errors.New("Invalid instruction"))
		return nil, errs
	}
}

func (f *FieldTypeArray2) GetMap(txn *badger.Txn, db *DB, params ...interface{}) (v interface{}, errs []error) {
	defer func() {
		r := recover()
		if r != nil {
			Log.Error().Interface("recovered", r).Stack().Str("stack", string(debug.Stack())).Msg("Recovered in ArrayField.GetMap")
			switch x := r.(type) {
			case string:
				errs = append(errs, errors.New(x))
			case error:
				errs = append(errs, x)
			default:
				errs = append(errs, errors.New("unknown error was thrown"))
			}
			return
		}
	}()
	errs = make([]error, 0)
	if db.Shotdown {
		errs = append(errs, errors.New("database closing..."))
		return nil, errs
	}
	isObj, ok := params[0].(bool)
	if !ok {
		errs = append(errs, errors.New("FieldArray.Get: First parameter not a bool"))
		return nil, errs
	}

	typ, ok := params[1].(string)
	if !ok {
		errs = append(errs, errors.New("FieldArray.Get: Second parameter not a string"))
		return nil, errs
	}

	field, ok := params[2].(string)
	if !ok {
		errs = append(errs, errors.New("FieldArray.Get: Third parameter not a string"))
		return nil, errs
	}
	var ao ArrayOptions2
	t := "o"
	if isObj {
		db.RLock()
		ao, ok = db.OT[typ].Fields[field].FieldTypeOptions[0].(ArrayOptions2)
		db.RUnlock()
	} else {
		t = "l"
		db.RLock()
		ao, ok = db.LT[typ].Fields[field].FieldTypeOptions[0].(ArrayOptions2)
		db.RUnlock()
	}
	if !ok {
		errs = append(errs, errors.New("FieldArray.Get: Field "+field+" Not found for Object/Link "+typ))
		return nil, errs
	}

	ins, ok := params[3].(string)
	if !ok {
		errs = append(errs, errors.New("FieldArray.GetMap: Fourth parameter not a string"))
		return nil, errs
	}

	switch ins {
	case "single":
		index := params[4].(string)

		id, err := db.FT["uint64"].Set(params[5])
		if err != nil {
			errs = append(errs, err)
			return nil, errs
		}
		idTO, err := db.FT["uint64"].Set(params[6])
		if err != nil {
			errs = append(errs, err)
			return nil, errs
		}
		opt := badger.DefaultIteratorOptions
		if isObj {
			opt.Prefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(id) + db.KV.D + field + db.KV.D + index)
		} else {
			opt.Prefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(id) + "-" + string(idTO) + db.KV.D + field + db.KV.D + index)
		}
		opt.PrefetchSize = 5
		opt.PrefetchValues = true
		iterator := txn.NewIterator(opt)
		defer iterator.Close()
		iterator.Seek(opt.Prefix)
		data := make(map[string]interface{})
		var k []byte
		var kArray [][]byte
		var v []byte
		for iterator.ValidForPrefix(opt.Prefix) {
			item := iterator.Item()
			k = item.KeyCopy(k)
			kArray = bytes.Split(k, []byte(db.KV.D))
			v, err = item.ValueCopy(nil)
			if err == nil {
				fieldKey := string(kArray[7])
				fieldKeyType := ao.Fields[fieldKey]
				db.RLock()
				ft, ok := db.FT[fieldKeyType]
				db.RUnlock()
				if ok {
					valField, err := ft.Get(v)
					if err == nil {
						data[fieldKey] = valField
					} else {
						Log.Error().Interface("error", err).Str("key", string(k)).Msg("converting value for key in Badger threw error")
					}
				} else {
					Log.Error().Str("key", string(k)).Msg("unable to get field type in array " + fieldKeyType)
				}
			} else {
				Log.Error().Interface("error", err).Interface("stack", debug.Stack()).Str("key", string(k)).Msg("Getting value for key in Badger threw error")
			}
			iterator.Next()
		}
		return data, errs
	case "list":
		objectID, err := db.FT["uint64"].Set(params[4])
		if err != nil {
			errs = append(errs, err)
			return nil, errs
		}
		objectIDTO, err := db.FT["uint64"].Set(params[5])
		if err != nil {
			errs = append(errs, err)
			return nil, errs
		}
		skip, ok := params[6].(int)
		if !ok {
			errs = append(errs, errors.New("sixth parameter is not an int"))
			return nil, errs
		}
		limit, ok := params[7].(int)
		if !ok {
			errs = append(errs, errors.New("seventh parameter is not an int"))
			return nil, errs
		}
		opt := badger.DefaultIteratorOptions
		if isObj {
			opt.Prefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(objectID) + db.KV.D + field)
		} else {
			opt.Prefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(objectID) + "-" + string(objectIDTO) + db.KV.D + field)
		}
		opt.PrefetchSize = 5
		opt.PrefetchValues = true
		iterator := txn.NewIterator(opt)
		defer iterator.Close()
		iterator.Seek(opt.Prefix)
		data := make(map[string]map[string]interface{})
		var k []byte
		var kArray [][]byte
		var v []byte
		currentID := ""
		skipped := 0
		for iterator.ValidForPrefix(opt.Prefix) {
			item := iterator.Item()
			k = item.KeyCopy(k)
			kArray = bytes.Split(k, []byte(db.KV.D))
			var idd string
			v, err = item.ValueCopy(nil)
			if err != nil {
				Log.Error().Interface("error", err).Interface("stack", debug.Stack()).Str("key", string(k)).Msg("Getting value for key in Badger threw error")
			} else {
				idd = string(kArray[6])
				if skipped >= skip && currentID != idd {
					_, ok := data[idd]
					if !ok {
						data[idd] = make(map[string]interface{})
					}
					fieldKey := string(kArray[7])
					fieldKeyType := ao.Fields[fieldKey]
					db.RLock()
					ft, ok := db.FT[fieldKeyType]
					db.RUnlock()
					if ok {
						valField, err := ft.Get(v)
						if err == nil {
							data[idd][fieldKey] = valField
						} else {
							Log.Error().Interface("error", err).Str("key", string(k)).Msg("converting value for key in Badger threw error")
						}
					}
				} else {
					if currentID != idd {
						skipped = skipped + 1
						currentID = idd
					}
				}
			}
			if len(data) > limit {
				delete(data, idd)
				break
			}
			iterator.Next()
		}

		return data, errs
	case "last":
		objID, err := db.FT["uint64"].Set(params[4])
		if err != nil {
			errs = append(errs, err)
			return nil, errs
		}
		objIDTO, err := db.FT["uint64"].Set(params[5])
		if err != nil {
			errs = append(errs, err)
			return nil, errs
		}
		le, ok := params[6].(int)
		if !ok {
			errs = append(errs, errors.New("seventh parameter is not int"))
			return nil, errs
		}
		opt := badger.DefaultIteratorOptions
		var originalPrefix []byte
		if isObj {
			originalPrefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(objID) + db.KV.D + field)
		} else {
			originalPrefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(objID) + "-" + string(objIDTO) + db.KV.D + field)
		}

		opt.Prefix = append(originalPrefix, 0xFF)

		opt.PrefetchSize = 100
		opt.PrefetchValues = true
		opt.Reverse = true
		iterator := txn.NewIterator(opt)
		defer iterator.Close()
		iterator.Seek(opt.Prefix)
		data := make(map[string]map[string]interface{})
		var k []byte
		var kArray [][]byte
		var v []byte
		run := true
		for run {
			item, err := getItem(iterator)
			if err != nil {
				break
			}
			if err == nil {
				k = item.KeyCopy(k)
				if !bytes.HasPrefix(k, originalPrefix) {
					break
				}
				kArray = bytes.Split(k, []byte(db.KV.D))
				v, err = item.ValueCopy(nil)
				var idd string
				if err != nil {
					Log.Error().Interface("error", err).Str("key", string(k)).Msg("Getting value for key in Badger threw error")
				} else {
					idd = string(kArray[6])
					_, ok := data[idd]
					if !ok {
						data[idd] = make(map[string]interface{})
					}
					fieldKey := string(kArray[7])
					fieldKeyType := ao.Fields[fieldKey]
					db.RLock()
					ft, ok := db.FT[fieldKeyType]
					db.RUnlock()
					if ok {
						valField, err := ft.Get(v)
						if err == nil {
							data[idd][fieldKey] = valField
						} else {
							Log.Error().Interface("error", err).Str("key", string(k)).Msg("converting value for key in Badger threw error")
						}
					}
				}
				if len(data) > le {
					delete(data, idd)
					break
				}
			}
			iterator.Next()

		}

		return data, errs
	default:
		errs = append(errs, errors.New("invalid instruction"))
		return nil, errs
	}
}

func (f *FieldTypeArray2) Compare(txn *badger.Txn, db *DB, isObj bool, typ string, id []byte, idTo []byte, field string, action string, param interface{}) (v bool, errs []error) {
	defer func() {
		r := recover()
		if r != nil {
			switch x := r.(type) {
			case string:
				errs = append(errs, errors.New(x))
			case error:
				errs = append(errs, x)
			default:
				errs = append(errs, errors.New("unknown error was thrown"))
			}
			return
		}
	}()
	errs = make([]error, 0)
	switch action {
	case "count-g":
		count, ok := param.(int)
		if !ok {
			errs = append(errs, errors.New("invalid parameter provided expecting int"))
			return false, errs
		}
		t := "o"
		if !isObj {
			t = "l"
		}
		opt := badger.DefaultIteratorOptions
		if isObj {
			opt.Prefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(id) + db.KV.D + field)
		} else {
			opt.Prefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(id) + "-" + string(idTo) + db.KV.D + field)
		}
		opt.PrefetchSize = 10
		opt.PrefetchValues = true
		iterator := txn.NewIterator(opt)
		defer iterator.Close()
		iterator.Seek(opt.Prefix)
		data := make(map[string]bool)
		var k []byte
		var kArray [][]byte
		for iterator.ValidForPrefix(opt.Prefix) {
			item := iterator.Item()
			k = item.KeyCopy(k)
			kArray = bytes.Split(k, []byte(db.KV.D))
			idd := string(kArray[6])
			data[idd] = false
			if len(data) > count {
				break
			}
			iterator.Next()
		}
		if len(data) >= count {
			return true, errs
		}
		return false, errs
	case "count-l":
		count, ok := param.(int)
		if !ok {
			errs = append(errs, errors.New("invalid parameter provided expecting int"))
			return false, errs
		}
		t := "o"
		if !isObj {
			t = "l"
		}
		opt := badger.DefaultIteratorOptions
		if isObj {
			opt.Prefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(id) + db.KV.D + field)
		} else {
			opt.Prefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(id) + "-" + string(idTo) + db.KV.D + field)
		}
		opt.PrefetchSize = 10
		opt.PrefetchValues = true
		iterator := txn.NewIterator(opt)
		defer iterator.Close()
		iterator.Seek(opt.Prefix)
		data := make(map[string]bool)
		var k []byte
		var kArray [][]byte
		for iterator.ValidForPrefix(opt.Prefix) {
			item := iterator.Item()
			k = item.KeyCopy(k)
			kArray = bytes.Split(k, []byte(db.KV.D))
			idd := string(kArray[6])
			data[idd] = false
			if len(data) > count {
				break
			}
			iterator.Next()
		}
		if len(data) < count {
			return true, errs
		}
		return false, errs
	default:
		secs := strings.Fields(action)
		if len(secs) < 2 {
			errs = append(errs, errors.New("Invalid instruction"))
			return false, errs
		}
		var arrayOptions2 ArrayOptions2
		var fieldName string
		var ok bool
		var fieldType FieldType
		db.RLock()
		if isObj {
			arrayOptions2, ok = db.OT[typ].Fields[field].FieldTypeOptions[0].(ArrayOptions2)
		} else {
			arrayOptions2, ok = db.LT[typ].Fields[field].FieldTypeOptions[0].(ArrayOptions2)
		}
		db.RUnlock()
		if !ok {
			errs = append(errs, errors.New("Invalid instruction"))
			return false, errs
		}
		fieldName, ok = arrayOptions2.Fields[secs[0]]
		if !ok {
			errs = append(errs, errors.New("The provided field is not found in the array of objects for this Field"))
			return false, errs
		}
		db.RLock()
		fieldType, ok = db.FT[fieldName]
		db.RUnlock()
		if !ok {
			errs = append(errs, errors.New("The provided field is linked to a fieldtype that is not in the database"))
			return false, errs
		}

		t := "o"
		if !isObj {
			t = "l"
		}
		opt := badger.DefaultIteratorOptions
		if isObj {
			opt.Prefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(id) + db.KV.D + field)
		} else {
			opt.Prefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(id) + "-" + string(idTo) + db.KV.D + field)
		}
		rawParam, err := fieldType.Set(param)
		if err != nil {
			errs = append(errs, err)
			return false, errs
		}
		opt.PrefetchSize = 10
		opt.PrefetchValues = true
		iterator := txn.NewIterator(opt)
		defer iterator.Close()
		iterator.Seek(opt.Prefix)
		var k []byte
		var kArray [][]byte
		for iterator.ValidForPrefix(opt.Prefix) {
			item := iterator.Item()
			k = item.KeyCopy(nil)
			kArray = bytes.Split(k, []byte(db.KV.D))
			v, err := item.ValueCopy(nil)
			if err != nil {
				Log.Error().Interface("error", err).Interface("stack", debug.Stack()).Str("key", string(k)).Msg("getting value for key in Badger threw error")
			} else {
				if string(kArray[7]) == secs[0] {
					ok, err := fieldType.Compare(secs[1], v, rawParam)
					if err != nil {
						errs = append(errs, errors.New("the provided field is linked to a fieldtype that is not in the database"))
						return false, errs
					}
					if ok {
						return true, errs
					}
				}
			}
			iterator.Next()
		}
		return false, errs
	}
}

func (f *FieldTypeArray2) Delete(txn *badger.Txn, db *DB, isObj bool, typ string, id []byte, idTo []byte, field string) (errs []error) {
	defer func() {
		r := recover()
		if r != nil {
			switch x := r.(type) {
			case string:
				errs = append(errs, errors.New(x))
			case error:
				errs = append(errs, x)
			default:
				errs = append(errs, errors.New("unknown error was thrown"))
			}
			return
		}
	}()
	errs = make([]error, 0)
	if db.Shotdown {
		errs = append(errs, errors.New("database closing..."))
		return errs
	}
	t := "o"
	if !isObj {
		t = "l"
	}
	opt := badger.DefaultIteratorOptions
	if isObj {
		opt.Prefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(id) + db.KV.D + field)
	} else {
		opt.Prefix = []byte(db.Options.DBName + db.KV.D + "a" + db.KV.D + t + db.KV.D + typ + db.KV.D + string(id) + "-" + string(idTo) + db.KV.D + field)
	}
	opt.PrefetchSize = 10
	opt.PrefetchValues = true
	iterator := txn.NewIterator(opt)
	defer iterator.Close()
	iterator.Seek(opt.Prefix)
	for iterator.ValidForPrefix(opt.Prefix) {
		item := iterator.Item()
		k := item.KeyCopy(nil)
		err := txn.Delete(k)
		if err != nil {
			errs = append(errs, err)
		}
		iterator.Next()
	}
	return errs
}

func (f *FieldTypeArray2) Close() error {
	return nil
}
