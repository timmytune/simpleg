package simpleg

import (
	"bytes"
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v2"
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
	s.DB.Lock.Lock()
	fieldIndexed := s.DB.OT[objectType].Fields[fieldName].Indexed
	allowIndexing := s.DB.FTO[s.DB.OT[objectType].Fields[fieldName].FieldType].AllowIndexing
	s.DB.Lock.Unlock()
	if fieldIndexed == true && allowIndexing == true {
		oldItem, oldErr := tnx.Get(s.DB.KV.CombineKey(s.DB.Options.DBName, objectType, string(id), fieldName))
		//If item does not exist in the db just create the new index only
		if oldErr != nil && oldErr == badger.ErrKeyNotFound {
			s.DB.KV.Writer2.Write(id, s.DB.Options.DBName, objectType, fieldName, string(value), string(id))
			return nil
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
		s.DB.KV.Writer2.Delete(s.DB.Options.DBName, objectType, fieldName, string(oldValue), string(id))
		//create new index

		s.DB.KV.Writer2.Write(id, s.DB.Options.DBName, objectType, fieldName, string(value), string(id))
	}
	return err
}

func (s *SetterFactory) object(typ string, o interface{}) (uint64, []error) {
	s.DB.Lock.Lock()
	ftUint64 := s.DB.FT["uint64"]
	ot, ok := s.DB.OT[typ]
	s.DB.Lock.Unlock()

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
		//s.DB.KV.DoneWriteTransaction()
	}()

	_, ok = m[KeyValueKey{Main: "ID"}]
	if !ok {
		i, ee := s.DB.KV.GetNextID(typ)
		if ee != nil {
			return i, append(e, ee)
		}
		m[KeyValueKey{Main: "ID"}] = ftUint64.Set(i)
		s.DB.KV.Writer2.Write(m[KeyValueKey{Main: "ID"}], s.DB.Options.DBName, typ, "ID", string(m[KeyValueKey{Main: "ID"}]), string(m[KeyValueKey{Main: "ID"}]))
	}

	ib = m[KeyValueKey{Main: "ID"}]
	delete(m, KeyValueKey{Main: "ID"})

	for key, v := range m {
		er = s.setObjectFieldIndex(tnx, typ, key.Main, v, ib)
		if er == nil {
			s.DB.KV.Writer2.Write(v, s.DB.Options.DBName, typ, string(ib), key.GetFullString(s.DB.KV.D))
		} else {
			e = append(e, er)
		}

	}

	//_ = s.DB.KV.FlushWrites()

	return i, e
}

func (s *SetterFactory) link(typ string, o interface{}) []error {
	s.DB.Lock.Lock()
	ftUint64 := s.DB.FT["uint64"]
	ot, ok := s.DB.LT[typ]
	s.DB.Lock.Unlock()
	var e []error
	if !ok {
		e = append(e, errors.New("link of type '"+typ+"' cannot be found in the database"))
		return e
	}

	v, e := ot.Validate(o, s.DB)

	if len(e) > 0 {
		return e
	}

	m, e := ot.Set(v, s.DB)
	if len(e) != 0 {
		return e
	}

	tnx := s.DB.KV.DB.NewTransaction(false)
	defer func() {
		tnx.Discard()
	}()

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

	_, err := tnx.Get(s.DB.KV.CombineKey(s.DB.Options.DBName, ot.Name, string(from), string(to), "FROM-TO"))

	ib = m[KeyValueKey{Main: "ID"}]
	delete(m, KeyValueKey{Main: "ID"})

	for key, v := range m {
		er = s.setObjectFieldIndex(tnx, typ, key.Main, v, ib)
		if er == nil {
			s.DB.KV.Writer2.Write(v, s.DB.Options.DBName, typ, string(ib), key.GetFullString(s.DB.KV.D))
		} else {
			e = append(e, er)
		}

	}

	//_ = s.DB.KV.FlushWrites()

	return i, e
}

func (s *SetterFactory) objectField(objectTypeName string, objectId uint64, fieldName string, fieldNewValue interface{}) (uint64, []error) {
	s.DB.Lock.Lock()
	ftUint64 := s.DB.FT["uint64"]
	objectTypeField, ok := s.DB.OT[objectTypeName].Fields[fieldName]
	fieldType := s.DB.FT[objectTypeField.FieldType]
	s.DB.Lock.Unlock()

	var er error
	var e []error

	if !ok {
		e = append(e, errors.New("object of type '"+objectTypeName+"' cannot be found in the database"))
		return objectId, e
	}

	ok, fieldNewValueValidated, er := objectTypeField.Validate(fieldNewValue, s.DB)
	fieldNewValueValidatedbytes := fieldType.Set(fieldNewValueValidated)

	if er != nil {
		e = append(e, er)
		return objectId, e
	}

	tnx := s.DB.KV.DB.NewTransaction(false)
	defer func() {
		tnx.Discard()
		//s.DB.KV.DoneWriteTransaction()
	}()
	er = s.setObjectFieldIndex(tnx, objectTypeName, fieldName, fieldNewValueValidatedbytes, ftUint64.Set(objectId))
	if er == nil {
		s.DB.KV.Writer2.Write(fieldNewValueValidatedbytes, s.DB.Options.DBName, objectTypeName, string(objectId), fieldName)
	} else {
		e = append(e, er)
	}

	return objectId, e
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
			fmt.Println("Recovered in Setter.Run ", r)

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
		switch job.Ins {

		case "save.object":
			id, err := s.object(job.Data[0].(string), job.Data[1])
			er.ID = id
			er.Errors = err
			if job.Ret != nil {
				job.Ret <- er
				close(job.Ret)
			}

		case "save.object.field":
			id, err := s.objectField(job.Data[0].(string), job.Data[1].(uint64), job.Data[2].(string), job.Data[3])
			er.ID = id
			er.Errors = err
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
