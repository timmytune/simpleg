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
	"fmt"
	"strconv"
)

type FieldTypeFloat64 struct {
}

func (f *FieldTypeFloat64) GetOption() map[string]string {
	m := make(map[string]string)
	m["Name"] = "Float64"
	m["AllowIndexing"] = "1"
	return m
}

func (f *FieldTypeFloat64) New() interface{} {
	var r float64 = 0
	return r
}

func (f *FieldTypeFloat64) Set(v interface{}) ([]byte, error) {
	d, ok := v.(float64)
	if !ok {
		return nil, errors.New("provided interface is not of type float64")
	}
	s := fmt.Sprintf("%g", d)
	return []byte(s), nil
}

func (f *FieldTypeFloat64) Get(v []byte) (interface{}, error) {
	d := string(v)
	if s, err := strconv.ParseFloat(d, 64); err == nil {
		return s, nil // 3.14159265
	} else {
		return nil, err
	}
}

func (f *FieldTypeFloat64) Compare(typ string, a []byte, b []byte) (bool, error) {
	var err error
	ia2, err := f.Get(a)
	if err != nil {
		return false, err
	}
	ib2, _ := f.Get(b)
	if err != nil {
		return false, err
	}
	ia, _ := ia2.(float64)
	ib, _ := ib2.(float64)
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
		return false, errors.New("fieldtype float64 does not support this comparison operator")
	}

}

func (f *FieldTypeFloat64) CompareIndexed(typ string, a interface{}) (string, string, error) {
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
		return "", "", errors.New("fieldtype float64 does not support this comparison operator for indexed field")
	}

}
