package simpleg

import (
	"encoding/binary"
	"errors"
	"strconv"
)

type FieldTypeUint64 struct {
}

func (f *FieldTypeUint64) GetOption() map[string]string {
	m := make(map[string]string)
	m["Name"] = "uint64"
	m["AllowIndexing"] = "1"
	return m
}

func (f *FieldTypeUint64) New() interface{} {
	var r uint64 = 0
	return r
}

func (f *FieldTypeUint64) Set(v interface{}) ([]byte, error) {
	d, ok := v.(uint64)
	if !ok {
		return nil, errors.New("Interface is not of type of uint64")
	}

	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, d)
	return buf, nil
}

func (f *FieldTypeUint64) Get(v []byte) (interface{}, error) {
	i, _ := binary.Uvarint(v)
	return i, nil
}

func (f *FieldTypeUint64) Compare(typ string, a []byte, b []byte) (bool, error) {
	var err error
	ia, _ := binary.Uvarint(a)
	ib, _ := binary.Uvarint(b)

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
		return false, errors.New("fieldtype uint64 does not support this comparison operator")
	}

}

func (f *FieldTypeUint64) CompareIndexed(typ string, a interface{}) (string, string, error) {
	var err error
	s := strconv.FormatUint(a.(uint64), 10)
	switch typ {
	case "==":
		return s, "=", err
	case ">":
		return s, "+", err
	case ">=":
		return s, "+=", err
	case "<":
		return s, "-", err
	case "<=":
		return s, "-=", err
	default:
		return "", "", errors.New("fieldtype uint64 does not support this comparison operator for indexed field")
	}

}
