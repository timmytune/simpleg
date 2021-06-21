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

	badger "github.com/dgraph-io/badger/v3"
)

type SetterJob struct {
	Ins  string
	Data []interface{}
	Ret  chan SetterRet
}

type SetterRet struct {
	ID     uint64
	Errors []error
}

type SetterFactory struct {
	DB    *DB
	Input chan SetterJob
}

func (s *SetterFactory) setObjectFieldIndex(tnx *badger.Txn, objectType string, fieldName string, value []byte, id []byte) error {

	var err error
	//check if indexing is allowed if not skip
	s.DB.RLock()
	fieldIndexed := s.DB.OT[objectType].Fields[fieldName].Indexed
	allowIndexing := s.DB.FTO[s.DB.OT[objectType].Fields[fieldName].FieldType].AllowIndexing
	s.DB.RUnlock()
	if fieldIndexed == true && allowIndexing == true {
		oldItem, oldErr := tnx.Get(s.DB.KV.CombineKey(s.DB.Options.DBName, objectType, fieldName, string(id)))
		//If item does not exist in the db just create the new index only
		if oldErr != nil && oldErr == badger.ErrKeyNotFound {
			err = s.DB.KV.Writer2.Write(id, s.DB.Options.DBName, objectType, fieldName, string(value), string(id))
			return err
		}
		//If another error that is not atype badger.ErrKeyNotFound just return the error
		if oldErr != nil && oldErr != badger.ErrKeyNotFound {
			return oldErr
		}
		oldValue, errOldValue := oldItem.ValueCopy(nil)
		//If there is an error copying the value just return the error returned
		if errOldValue != nil {
			return errOldValue
		}
		//Value has not changed so no need to update index
		if bytes.Compare(oldValue, value) == 0 {
			return nil
		}
		//Delete old index
		err = s.DB.KV.Writer2.Delete(s.DB.Options.DBName, objectType, fieldName, string(oldValue), string(id))
		//create new index

		err = s.DB.KV.Writer2.Write(id, s.DB.Options.DBName, objectType, fieldName, string(value), string(id))
	}
	return err
}

func (s *SetterFactory) object(typ string, o interface{}) (uint64, []error) {
	s.DB.RLock()
	ftUint64 := s.DB.FT["uint64"]
	ot, ok := s.DB.OT[typ]
	s.DB.RUnlock()

	var er error
	var e []error
	var i uint64
	var ib []byte
	if !ok {
		e = append(e, errors.New("object of type '"+typ+"' cannot be found in the database"))
		return i, e
	}

	v, e := ot.Validate(o, s.DB)

	if len(e) > 0 {
		return i, e
	}
	m, e := ot.Set(v, s.DB)
	if len(e) != 0 {
		return i, e
	}

	tnx := s.DB.KV.DB.NewTransaction(false)
	defer func() {
		tnx.Discard()
	}()

	_, ok = m[KeyValueKey{Main: "ID"}]
	if !ok {
		var ee error
		i, ee = s.DB.KV.GetNextID(typ, 100)
		if ee != nil {
			return i, append(e, ee)
		}
		m[KeyValueKey{Main: "ID"}], _ = ftUint64.Set(i)
		er = s.DB.KV.Writer2.Write(m[KeyValueKey{Main: "ID"}], s.DB.Options.DBName, typ, "ID", string(m[KeyValueKey{Main: "ID"}]), string(m[KeyValueKey{Main: "ID"}]))
		if er != nil {
			e = append(e, er)
			return i, e
		}
	}
	ib = m[KeyValueKey{Main: "ID"}]
	delete(m, KeyValueKey{Main: "ID"})
	for key, v := range m {
		er := s.setObjectFieldIndex(tnx, typ, key.Main, v, ib)
		if er == nil {
			er = s.DB.KV.Writer2.Write(v, s.DB.Options.DBName, typ, string(ib), key.GetFullString(s.DB.KV.D))
			if er != nil {
				e = append(e, er)
			}
		} else {
			e = append(e, er)
		}
	}
	return i, e
}

func (s *SetterFactory) link(typ string, o interface{}) []error {
	s.DB.RLock()
	//ftUint64 := s.DB.FT["uint64"]
	lt, ok := s.DB.LT[typ]
	s.DB.RUnlock()
	var e []error
	if !ok {
		e = append(e, errors.New("link of type '"+typ+"' cannot be found in the database"))
		return e
	}

	v, e := lt.Validate(o, s.DB)

	if len(e) > 0 {
		return e
	}

	m, e := lt.Set(v, s.DB)
	if len(e) != 0 {
		return e
	}

	tnx := s.DB.KV.DB.NewTransaction(false)
	defer tnx.Discard()

	from, ok := m[KeyValueKey{Main: "FROM"}]
	if !ok {
		e = append(e, errors.New("FROM field is not provided"))
		return e
	}

	to, ok := m[KeyValueKey{Main: "TO"}]
	if !ok {
		e = append(e, errors.New("TO field is not provided"))
		return e
	}

	_, err := tnx.Get(s.DB.KV.CombineKey(s.DB.Options.DBName, lt.From, "ID", string(from), string(from)))
	if err != nil {
		e = append(e, err)
	}
	_, err = tnx.Get(s.DB.KV.CombineKey(s.DB.Options.DBName, lt.To, "ID", string(to), string(to)))
	if err != nil {
		e = append(e, err)
	}
	data, err := tnx.Get(s.DB.KV.CombineKey(s.DB.Options.DBName, typ, "INDEXED+", string(to), string(from)))
	// if opposites are the same just use prexisting link

	if data != nil && (lt.Type == 1 || lt.Type == 2) {
		h := to
		to = from
		from = h
	}
	if len(e) < 1 {
		delete(m, KeyValueKey{Main: "FROM"})
		delete(m, KeyValueKey{Main: "TO"})
		for key, v := range m {
			err := s.DB.KV.Writer2.Write(v, s.DB.Options.DBName, typ, string(from), string(to), key.GetFullString(s.DB.KV.D))
			if err != nil {
				e = append(e, err)
			}
		}
		err := s.DB.KV.Writer2.Write(make([]byte, 0), s.DB.Options.DBName, typ, "INDEXED-", string(to), string(from))
		if err != nil {
			e = append(e, err)
		}
		err = s.DB.KV.Writer2.Write(make([]byte, 0), s.DB.Options.DBName, typ, "INDEXED+", string(from), string(to))
		if err != nil {
			e = append(e, err)
		}
	}
	return e
}

func (s *SetterFactory) objectField(objectTypeName string, objectId uint64, fieldName string, fieldNewValue interface{}) (uint64, []error) {
	s.DB.RLock()
	ftUint64 := s.DB.FT["uint64"]
	objectTypeField, ok := s.DB.OT[objectTypeName].Fields[fieldName]
	fieldType := s.DB.FT[objectTypeField.FieldType]
	s.DB.RUnlock()

	var er error
	var e []error

	if !ok {
		e = append(e, errors.New("object of type '"+objectTypeName+"' cannot be found in the database"))
		return objectId, e
	}
	var fieldNewValueValidated interface{}
	if objectTypeField.Validate != nil {
		ok, fieldNewValueValidated, er = objectTypeField.Validate(fieldNewValue, s.DB)
	} else {
		fieldNewValueValidated = fieldNewValue
	}

	fieldNewValueValidatedbytes, err2 := fieldType.Set(fieldNewValueValidated)

	if err2 != nil {
		e = append(e, err2)
		return objectId, e
	}

	if er != nil {
		e = append(e, er)
		return objectId, e
	}

	tnx := s.DB.KV.DB.NewTransaction(false)
	defer func() {
		tnx.Discard()
		//s.DB.KV.DoneWriteTransaction()
	}()
	idRaw, err3 := ftUint64.Set(objectId)
	if err3 != nil {
		e = append(e, err3)
		return objectId, e
	}
	er = s.setObjectFieldIndex(tnx, objectTypeName, fieldName, fieldNewValueValidatedbytes, idRaw)
	if er == nil {
		er = s.DB.KV.Writer2.Write(fieldNewValueValidatedbytes, s.DB.Options.DBName, objectTypeName, string(idRaw), fieldName)
		if er != nil {
			e = append(e, er)
		}
	} else {
		e = append(e, er)
	}

	return objectId, e
}

func (s *SetterFactory) linkField(linkTypeName string, from uint64, to uint64, fieldName string, fieldNewValue interface{}) []error {
	s.DB.RLock()
	ftUint64 := s.DB.FT["uint64"]
	linkType, ok := s.DB.LT[linkTypeName]
	s.DB.RUnlock()

	var er error
	var e []error
	if !ok {
		e = append(e, errors.New("link of type '"+linkTypeName+"' cannot be found in the database"))
		return e
	}
	linkTypeField, ok1 := linkType.Fields[fieldName]
	if !ok1 {
		e = append(e, errors.New("linkField of type '"+linkTypeName+" | "+fieldName+"' cannot be found in the database"))
		return e
	}
	fieldType, ok2 := s.DB.FT[linkTypeField.FieldType]
	if !ok2 {
		e = append(e, errors.New("field of type '"+linkTypeField.FieldType+"' cannot be found in the database"))
		return e
	}

	var fieldNewValueValidated interface{}
	if linkTypeField.Validate != nil {
		ok, fieldNewValueValidated, er = linkTypeField.Validate(fieldNewValue, s.DB)
	} else {
		fieldNewValueValidated = fieldNewValue
	}

	fieldNewValueValidatedbytes, err2 := fieldType.Set(fieldNewValueValidated)

	if err2 != nil {
		e = append(e, err2)
	}

	if er != nil {
		e = append(e, er)
	}
	if len(e) > 0 {
		return e
	}
	tnx := s.DB.KV.DB.NewTransaction(false)
	defer func() {
		tnx.Discard()
		//s.DB.KV.DoneWriteTransaction()
	}()
	fromRaw, err3 := ftUint64.Set(from)
	if err3 != nil {
		e = append(e, err3)
	}
	toRaw, err4 := ftUint64.Set(to)
	if err4 != nil {
		e = append(e, err4)
	}
	_, err := tnx.Get(s.DB.KV.CombineKey(s.DB.Options.DBName, linkType.From, "ID", string(fromRaw), string(fromRaw)))
	if err != nil {
		e = append(e, err)
	}
	_, err = tnx.Get(s.DB.KV.CombineKey(s.DB.Options.DBName, linkType.To, "ID", string(toRaw), string(toRaw)))
	if err != nil {
		e = append(e, err)
	}

	err = s.DB.KV.Writer2.Write(fieldNewValueValidatedbytes, s.DB.Options.DBName, linkTypeName, string(fromRaw), string(toRaw), fieldName)
	if err != nil {
		e = append(e, err)
	}
	return e
}

func (s *SetterFactory) DeleteObjectField(object string, objectID []byte, field string, val []byte, fieldTypeOptions FieldOptions) []error {
	errs := make([]error, 0)
	if !fieldTypeOptions.Advanced {
		if fieldTypeOptions.Indexed {
			if val != nil {
				s.DB.KV.Writer2.Delete(s.DB.Options.DBName, object, field, string(val), string(objectID))
			} else {
				txn := s.DB.KV.DB.NewTransaction(true)
				defer txn.Discard()
				opt := badger.DefaultIteratorOptions
				opt.Prefix = []byte(s.DB.Options.DBName + s.DB.KV.D + object + s.DB.KV.D + field)
				opt.PrefetchSize = 10
				opt.PrefetchValues = true
				iterator := txn.NewIterator(opt)
				iterator.Seek(opt.Prefix)
				for iterator.ValidForPrefix(opt.Prefix) {
					item := iterator.Item()
					k := item.KeyCopy(nil)
					kArray := bytes.Split(k, []byte(s.DB.KV.D))
					if string(kArray[4]) == string(objectID) {
						err := txn.Delete(k)
						if err != nil {
							errs = append(errs, err)
						}
						break
					}
					iterator.Next()
				}
				iterator.Close()
				err := txn.Commit()
				if err != nil {
					errs = append(errs, err)
				}
			}
		}
		err := s.DB.KV.Writer2.Delete(s.DB.Options.DBName, object, string(objectID), field)
		if err != nil {
			errs = append(errs, err)
		}
	} else {
		s.DB.RLock()
		advancedFieldType := s.DB.AFT[fieldTypeOptions.FieldType]
		s.DB.RUnlock()
		txn := s.DB.KV.DB.NewTransaction(true)
		defer txn.Discard()
		errs2 := advancedFieldType.Delete(txn, s.DB, true, object, objectID, objectID, field)
		if len(errs) > 0 {
			errs = append(errs, errs2...)
		}
		err := txn.Commit()
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (s *SetterFactory) DeleteLinkField(object string, objectFrom []byte, objectTo []byte, field string, fieldTypeOptions FieldOptions) []error {
	errs := make([]error, 0)
	if !fieldTypeOptions.Advanced {
		err := s.DB.KV.Writer2.Delete(s.DB.Options.DBName, object, string(objectFrom), string(objectTo), field)
		if err != nil {
			errs = append(errs, err)
		}
	} else {
		s.DB.RLock()
		advancedFieldType := s.DB.AFT[fieldTypeOptions.FieldType]
		s.DB.RUnlock()
		txn := s.DB.KV.DB.NewTransaction(true)
		defer txn.Discard()
		errs2 := advancedFieldType.Delete(txn, s.DB, false, object, objectFrom, objectTo, field)
		if len(errs) > 0 {
			errs = append(errs, errs2...)
		}
		err := txn.Commit()
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (s *SetterFactory) DeleteObject(object string, objectID []byte) []error {
	errs := make([]error, 0)
	s.DB.RLock()
	objectType, ok := s.DB.OT[object]
	links := s.DB.LT
	s.DB.RUnlock()
	if !ok {
		errs = append(errs, errors.New("object of type '"+object+"' not found in DB"))
	}
	txn := s.DB.KV.DB.NewTransaction(true)
	defer txn.Discard()
	for k, v := range links {
		if v.From == object || v.To == object {
			opt := badger.DefaultIteratorOptions
			opt.Prefix = []byte(s.DB.Options.DBName + s.DB.KV.D + k + s.DB.KV.D + "INDEXED+" + s.DB.KV.D + string(objectID))
			opt.PrefetchSize = 10
			opt.PrefetchValues = false
			iterator := txn.NewIterator(opt)
			iterator.Seek(opt.Prefix)
			for iterator.ValidForPrefix(opt.Prefix) {
				item := iterator.Item()
				key := item.KeyCopy(nil)
				kArray := bytes.Split(key, []byte(s.DB.KV.D))
				es := s.DeleteLink(k, kArray[3], kArray[4])
				if es != nil {
					errs = append(errs, es...)
				}
				iterator.Next()
			}
			iterator.Close()
			opt2 := badger.DefaultIteratorOptions
			opt2.PrefetchSize = 10
			opt2.PrefetchValues = false
			opt2.Prefix = []byte(s.DB.Options.DBName + s.DB.KV.D + k + s.DB.KV.D + "INDEXED-" + s.DB.KV.D + string(objectID))
			iterator2 := txn.NewIterator(opt2)
			iterator2.Seek(opt.Prefix)
			for iterator2.ValidForPrefix(opt.Prefix) {
				item := iterator2.Item()
				key := item.KeyCopy(nil)
				kArray := bytes.Split(key, []byte(s.DB.KV.D))
				es := s.DeleteLink(k, kArray[3], kArray[4])
				if es != nil {
					errs = append(errs, es...)
				}
				iterator2.Next()
			}
			iterator2.Close()
		}
	}
	opt := badger.DefaultIteratorOptions
	opt.Prefix = []byte(s.DB.Options.DBName + s.DB.KV.D + object + s.DB.KV.D + string(objectID))
	opt.PrefetchSize = 10
	opt.PrefetchValues = true
	iterator3 := txn.NewIterator(opt)
	iterator3.Seek(opt.Prefix)
	var k []byte
	for iterator3.ValidForPrefix(opt.Prefix) {
		item := iterator3.Item()
		k = item.KeyCopy(nil)
		err := txn.Delete(k)
		if err != nil {
			errs = append(errs, err)
		}
		karray := bytes.Split(k, []byte(s.DB.KV.D))
		if objectType.Fields[string(karray[3])].Indexed {
			v, err := item.ValueCopy(nil)
			if err != nil {
				errs = append(errs, err)
			}
			err = txn.Delete([]byte(s.DB.Options.DBName + s.DB.KV.D + object + s.DB.KV.D + string(karray[3]) + s.DB.KV.D + string(v) + s.DB.KV.D + string(objectID)))
			if err != nil {
				errs = append(errs, err)
			}
		}
		iterator3.Next()
	}
	iterator3.Close()
	for k, v := range objectType.Fields {
		if v.Advanced {
			s.DB.RLock()
			aft := s.DB.AFT[v.FieldType]
			s.DB.RUnlock()
			es := aft.Delete(txn, s.DB, true, object, objectID, objectID, k)
			if len(es) > 0 {
				errs = append(errs, es...)
			}
		}
	}
	err := txn.Delete([]byte(s.DB.Options.DBName + s.DB.KV.D + object + s.DB.KV.D + "ID" + s.DB.KV.D + string(objectID) + s.DB.KV.D + string(objectID)))
	if err != nil {
		errs = append(errs, err)
	}
	err = txn.Commit()
	if err != nil {
		errs = append(errs, err)
	}
	return errs
}

func (s *SetterFactory) DeleteLink(object string, objectFrom []byte, objectTo []byte) []error {
	errs := make([]error, 0)
	s.DB.RLock()
	link, ok := s.DB.LT[object]
	s.DB.RUnlock()
	if !ok {
		errs = append(errs, errors.New("link of type '"+object+"' not found in DB"))
	}
	txn := s.DB.KV.DB.NewTransaction(true)
	defer txn.Discard()

	opt := badger.DefaultIteratorOptions
	opt.Prefix = []byte(s.DB.Options.DBName + s.DB.KV.D + object + s.DB.KV.D + string(objectFrom) + s.DB.KV.D + string(objectTo))
	opt.PrefetchSize = 10
	opt.PrefetchValues = true
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
	if link.Type == 1 || link.Type == 2 {
		opt := badger.DefaultIteratorOptions
		opt.Prefix = []byte(s.DB.Options.DBName + s.DB.KV.D + object + s.DB.KV.D + string(objectTo) + s.DB.KV.D + string(objectFrom))
		opt.PrefetchSize = 10
		opt.PrefetchValues = true
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
	err := s.DB.KV.Writer2.Delete(s.DB.Options.DBName, object, "INDEXED+", string(objectFrom), string(objectTo))
	if err != nil {
		errs = append(errs, err)
	}
	err = s.DB.KV.Writer2.Delete(s.DB.Options.DBName, object, "INDEXED-", string(objectTo), string(objectFrom))
	if err != nil {
		errs = append(errs, err)
	}

	if link.Type == 1 || link.Type == 2 {
		err := s.DB.KV.Writer2.Delete(s.DB.Options.DBName, object, "INDEXED+", string(objectTo), string(objectFrom))
		if err != nil {
			errs = append(errs, err)
		}
		err = s.DB.KV.Writer2.Delete(s.DB.Options.DBName, object, "INDEXED-", string(objectFrom), string(objectTo))
		if err != nil {
			errs = append(errs, err)
		}
	}

	for k, v := range link.Fields {
		if v.Advanced {
			s.DB.RLock()
			aft := s.DB.AFT[v.FieldType]
			s.DB.RUnlock()
			es := aft.Delete(txn, s.DB, false, object, objectFrom, objectTo, k)
			if link.Type == 1 || link.Type == 2 {
				es = aft.Delete(txn, s.DB, false, object, objectTo, objectFrom, k)
			}
			if len(es) > 0 {
				errs = append(errs, es...)
			}

		}
	}
	err = txn.Commit()
	if err != nil {
		errs = append(errs, err)
	}
	return errs
}

func (s *SetterFactory) Start(db *DB, numOfRuners int, inputChannelLength int) {
	s.DB = db
	s.Input = make(chan SetterJob, inputChannelLength)
	for i := 0; i < numOfRuners; i++ {
		go s.Run()
	}
}

func (s *SetterFactory) Run() {
	var job SetterJob
	var er SetterRet

	defer func() {
		r := recover()
		if r != nil {
			Log.Error().Interface("recovered", r).Stack().Interface("stack", string(debug.Stack())).Msg("Recovered in Setter.Run ")
			if er.Errors == nil {
				er.Errors = make([]error, 0)
			}
			switch x := r.(type) {
			case string:
				er.Errors = append(er.Errors, errors.New(x))
			case error:
				er.Errors = append(er.Errors, x)
			default:
				er.Errors = append(er.Errors, errors.New("Unknown error was thrown"))
			}
			job.Ret <- er
			close(job.Ret)
			s.Run()
		}
	}()
	for {
		job = <-s.Input
		er = SetterRet{Errors: make([]error, 0)}
		switch job.Ins {
		case "save.object":
			one, ok := job.Data[0].(string)
			if !ok {
				er.Errors = append(er.Errors, errors.New("Invalid first argument provided in nodequery"))
			}
			id, err := s.object(one, job.Data[1])
			er.ID = id
			er.Errors = append(er.Errors, err...)
			if job.Ret != nil {
				job.Ret <- er
				close(job.Ret)
			}
		case "save.object.field":
			one, ok := job.Data[0].(string)
			if !ok {
				er.Errors = append(er.Errors, errors.New("Invalid first argument provided in nodequery"))
			}
			two, ok := job.Data[1].(uint64)
			if !ok {
				er.Errors = append(er.Errors, errors.New("Invalid first argument provided in nodequery"))
			}
			three, ok := job.Data[2].(string)
			if !ok {
				er.Errors = append(er.Errors, errors.New("Invalid first argument provided in nodequery"))
			}
			id, err := s.objectField(one, two, three, job.Data[3])
			er.ID = id
			er.Errors = err
			if job.Ret != nil {
				job.Ret <- er
				close(job.Ret)
			}
		case "save.link":
			one, ok := job.Data[0].(string)
			if !ok {
				er.Errors = append(er.Errors, errors.New("invalid first argument provided in nodequery"))
			}
			err := s.link(one, job.Data[1])
			er.Errors = err
			if job.Ret != nil {
				job.Ret <- er
				close(job.Ret)
			}

		case "save.link.field":
			one, ok := job.Data[0].(string)
			if !ok {
				er.Errors = append(er.Errors, errors.New("Invalid first argument provided in nodequery"))
			}
			two, ok := job.Data[1].(uint64)
			if !ok {
				er.Errors = append(er.Errors, errors.New("Invalid first argument provided in nodequery"))
			}
			three, ok := job.Data[2].(uint64)
			if !ok {
				er.Errors = append(er.Errors, errors.New("Invalid first argument provided in nodequery"))
			}
			four, ok := job.Data[3].(string)
			if !ok {
				er.Errors = append(er.Errors, errors.New("Invalid first argument provided in nodequery"))
			}
			err := s.linkField(one, two, three, four, job.Data[4])
			er.Errors = err
			if job.Ret != nil {
				job.Ret <- er
				close(job.Ret)
			}
		case "delete.object.field":
			object, ok := job.Data[0].(string)
			if !ok {
				er.Errors = append(er.Errors, errors.New("invalid first argument provided in nodequery"))
			}
			field, ok := job.Data[2].(string)
			if !ok {
				er.Errors = append(er.Errors, errors.New("invalid first argument provided in nodequery"))
			}
			var val []byte
			var err2 error
			s.DB.RLock()
			objectID, err := s.DB.FT["uint64"].Set(job.Data[1])
			fieldTypeOptions, ok := s.DB.OT[object].Fields[field]
			if len(job.Data) >= 4 {
				val, err2 = s.DB.FT[fieldTypeOptions.FieldType].Set(job.Data[3])
			}
			s.DB.RUnlock()
			if err != nil {
				er.Errors = append(er.Errors, err)
			}
			if err2 != nil {
				er.Errors = append(er.Errors, err2)
			}
			if !ok {
				er.Errors = append(er.Errors, errors.New("field not found for this Object in database"))
			}
			if len(er.Errors) < 1 {
				errs := s.DeleteObjectField(object, objectID, field, val, fieldTypeOptions)
				if len(errs) > 0 {
					er.Errors = append(er.Errors, errs...)
				}
			}
			if job.Ret != nil {
				job.Ret <- er
				close(job.Ret)
			}
		case "delete.link.field":
			object, ok := job.Data[0].(string)
			if !ok {
				er.Errors = append(er.Errors, errors.New("Invalid first argument provided in nodequery"))
			}
			field, ok := job.Data[3].(string)
			if !ok {
				er.Errors = append(er.Errors, errors.New("Invalid first argument provided in nodequery"))
			}
			s.DB.RLock()
			objectFrom, err := s.DB.FT["uint64"].Set(job.Data[1])
			objectTo, err2 := s.DB.FT["uint64"].Set(job.Data[2])
			fieldTypeOptions, ok := s.DB.LT[object].Fields[field]
			s.DB.RUnlock()
			if err != nil {
				er.Errors = append(er.Errors, err)
			}
			if err2 != nil {
				er.Errors = append(er.Errors, err2)
			}
			if !ok {
				er.Errors = append(er.Errors, errors.New("Field not found for this Link in database"))
			}
			if len(er.Errors) < 1 {
				errs := s.DeleteLinkField(object, objectFrom, objectTo, field, fieldTypeOptions)
				if len(errs) > 0 {
					er.Errors = append(er.Errors, errs...)
				}
			}
			if job.Ret != nil {
				job.Ret <- er
				close(job.Ret)
			}
		case "delete.link":
			object, ok := job.Data[0].(string)
			if !ok {
				er.Errors = append(er.Errors, errors.New("Invalid first argument provided in nodequery"))
			}
			s.DB.RLock()
			objectFrom, err := s.DB.FT["uint64"].Set(job.Data[1])
			objectTo, err2 := s.DB.FT["uint64"].Set(job.Data[2])
			s.DB.RUnlock()
			if err != nil {
				er.Errors = append(er.Errors, err)
			}
			if err2 != nil {
				er.Errors = append(er.Errors, err2)
			}
			if len(er.Errors) < 1 {
				errs := s.DeleteLink(object, objectFrom, objectTo)
				if len(errs) > 0 {
					er.Errors = append(er.Errors, errs...)
				}
			}
			if job.Ret != nil {
				job.Ret <- er
				close(job.Ret)
			}
		case "delete.object":
			object, ok := job.Data[0].(string)
			if !ok {
				er.Errors = append(er.Errors, errors.New("invalid first argument provided in query argument"))
			}
			s.DB.RLock()
			objectID, err := s.DB.FT["uint64"].Set(job.Data[1])
			s.DB.RUnlock()
			if err != nil {
				er.Errors = append(er.Errors, err)
			}
			if len(er.Errors) < 1 {
				errs := s.DeleteObject(object, objectID)
				if len(errs) > 0 {
					er.Errors = append(er.Errors, errs...)
				}
			}
			if job.Ret != nil {
				job.Ret <- er
				close(job.Ret)
			}
		default:
			er.Errors = make([]error, 0)
			er.Errors = append(er.Errors, errors.New("Invalid Instruction provided "+job.Ins))
			if job.Ret != nil {
				job.Ret <- er
				close(job.Ret)
			}
		}

	}

}
