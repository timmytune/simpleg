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
		return nil, errors.New("interface is not of type of uint64")
	}
	var buf []byte
	buf = strconv.AppendUint(buf, d, 10)
	l := len(buf)
	var buf2 []byte
	buf2 = []byte(strconv.FormatUint(uint64(l), 32))
	buf2 = append(buf2, buf...)
	return buf2, nil
}

func (f *FieldTypeUint64) Get(v []byte) (interface{}, error) {
	vv := v[1:]
	i, _ := strconv.ParseUint(string(vv), 10, 64)
	return i, nil
}

func (f *FieldTypeUint64) Compare(typ string, a []byte, b []byte) (bool, error) {
	var err error
	av := a[1:]
	ia, _ := strconv.ParseUint(string(av), 10, 64)
	bv := b[1:]
	ib, _ := strconv.ParseUint(string(bv), 10, 64)

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
		return "", "", errors.New("fieldtype uint64 does not support this comparison operator for indexed field")
	}

}
