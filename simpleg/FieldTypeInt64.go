package simpleg

import (
	"encoding/binary"
	"errors"
)

type FieldTypeInt64 struct {
}

func (f *FieldTypeInt64) GetOption() map[string]string {
	m := make(map[string]string)
	m["Name"] = "int64"
	m["AllowIndexing"] = "1"
	return m
}

func (f *FieldTypeInt64) New() interface{} {
	var r int64 = 0
	return r
}

func (f *FieldTypeInt64) Set(v interface{}) ([]byte, error) {
	d, ok := v.(int64)
	if !ok {
		return nil, errors.New("Provided interface is not of type int64")
	}
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(buf, d)
	return buf, nil
}

func (f *FieldTypeInt64) Get(v []byte) (interface{}, error) {
	i, _ := binary.Varint(v)
	return i, nil
}

func (f *FieldTypeInt64) Compare(typ string, a []byte, b []byte) (bool, error) {
	var err error
	ia, _ := binary.Varint(a)
	ib, _ := binary.Varint(b)

	switch typ {
	case "==":
		return (ia == ib), err
	case "!=":
		return (ia != ib), err
	case ">":
		return (ia > ib), err
	case ">=":
		return (ia >= ib), err
	case "<":
		return (ia < ib), err
	case "<=":
		return (ia <= ib), err
	default:
		return false, errors.New("fieldtype int64 does not support this comparison operator")
	}

}

func (f *FieldTypeInt64) CompareIndexed(typ string, a interface{}) (string, string, error) {
	var err error
	g, err := f.Set(a)
	if err != nil {
		return "", "", err
	}
	s := string(g)
	switch typ {
	case "==":
		return s, "==", err
	case ">":
		return s, ">", err
	case ">=":
		return s, ">=", err
	case "<":
		return s, "<", err
	default:
		return "", "", errors.New("fieldtype int64 does not support this comparison operator for indexed field")
	}

}
