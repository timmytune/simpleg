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
