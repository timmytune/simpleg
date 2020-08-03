package simpleg

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type FieldTypeBool struct {
}

func (f *FieldTypeBool) GetOption() map[string]string {
	m := make(map[string]string)
	m["Name"] = "bool"
	m["AllowIndexing"] = "0"
	return m
}

func (f *FieldTypeBool) New() interface{} {
	return false
}

func (f *FieldTypeBool) Set(v interface{}) []byte {
	b := make([]byte, 4)
	d, _ := v.(bool)
	if d {
		binary.LittleEndian.PutUint16(b, 1)
	} else {
		binary.LittleEndian.PutUint16(b, 0)
	}
	return b
}

func (f *FieldTypeBool) Get(v []byte) interface{} {

	if binary.LittleEndian.Uint16(v) == 1 {
		return true
	}
	return false
}

func (f *FieldTypeBool) Compare(typ string, a []byte, b []byte) (bool, error) {
	var err error
	if typ == "==" {
		return f.equal(a, b), err
	}
	if typ == "!=" {
		return f.notEqual(a, b), err
	}
	return false, errors.New("type bool does not support this comparison operator")
}

func (f *FieldTypeBool) CompareIndexed(typ string, a interface{}) (string, string, error) {
	return "", "", errors.New("fieldtype bool does not support this comparison operator for indexed field")
}

func (f *FieldTypeBool) equal(a, b []byte) bool {
	return bytes.Compare(a, b) == 0
}

func (f *FieldTypeBool) notEqual(a, b []byte) bool {
	return bytes.Compare(a, b) != 0
}
