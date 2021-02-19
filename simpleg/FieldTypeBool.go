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

func (f *FieldTypeBool) Set(v interface{}) ([]byte, error) {
	b := make([]byte, 4)
	d, ok := v.(bool)
	if !ok {
		return b, errors.New("Provided interface is not of type bool")
	}
	if d {
		binary.LittleEndian.PutUint16(b, 1)
	} else {
		binary.LittleEndian.PutUint16(b, 0)
	}
	return b, nil
}

func (f *FieldTypeBool) Get(v []byte) (interface{}, error) {

	if binary.LittleEndian.Uint16(v) == 1 {
		return true, nil
	}
	return false, nil
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
