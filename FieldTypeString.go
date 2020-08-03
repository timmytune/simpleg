package simpleg

import (
	"bytes"
	"errors"
)

type FieldTypeString struct {
}

func (f *FieldTypeString) GetOption() map[string]string {
	m := make(map[string]string)
	m["Name"] = "string"
	m["AllowIndexing"] = "1"
	return m
}

func (f *FieldTypeString) New() interface{} {
	return ""
}

func (f *FieldTypeString) Set(v interface{}) []byte {
	d := v.(string)
	return []byte(d)
}

func (f *FieldTypeString) Get(v []byte) interface{} {
	return string(v)
}

func (f *FieldTypeString) Compare(typ string, a []byte, b []byte) (bool, error) {
	var err error

	switch typ {
	case "==":
		return (bytes.Compare(a, b) == 0), err
	case "!=":
		return (bytes.Compare(a, b) != 0), err
	case "contains":
		return bytes.Contains(a, b), err
	case "NoCaseEqual":
		return bytes.EqualFold(a, b), err
	case "HasSuffix":
		return bytes.HasSuffix(a, b), err
	default:
		return false, errors.New("fieldtype string does not support this comparison operator")
	}

}

func (f *FieldTypeString) CompareIndexed(typ string, a interface{}) (string, string, error) {
	var err error
	b, _ := a.(string)
	switch typ {
	case "==":
		return b, "", err
	case "prefix":
		return b, "", err
	default:
		return "", "", errors.New("fieldtype string does not support this comparison operator for indexed field")
	}

}
