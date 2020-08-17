package simpleg

import (
	"encoding/binary"
	"errors"
	"strconv"
	"time"
)

type FieldTypeDate struct {
}

func (f *FieldTypeDate) GetOption() map[string]string {
	m := make(map[string]string)
	m["Name"] = "date"
	m["AllowIndexing"] = "1"
	return m
}

func (f *FieldTypeDate) New() interface{} {
	return time.Now()
}

func (f *FieldTypeDate) Set(v interface{}) ([]byte, error) {
	d, ok := v.(time.Time)
	if !ok {
		return nil, errors.New("Provided interface is not of type Time")
	}
	t, err := d.MarshalJSON()
	return t, err
}

func (f *FieldTypeDate) Get(v []byte) (interface{}, error) {
	t := time.Now()
	err := t.UnmarshalText(v)
	if err != nil {
		return t, err
	}
	return t, err
}

func (f *FieldTypeDate) Compare(typ string, a []byte, b []byte) (bool, error) {
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

func (f *FieldTypeDate) CompareIndexed(typ string, a interface{}) (string, string, error) {
	var err error
	s := strconv.FormatInt(a.(int64), 10)
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
		return "", "", errors.New("fieldtype int64 does not support this comparison operator for indexed field")
	}

}
