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
		return nil, errors.New("provided interface is not of type time.Time")
	}
	t, err := d.MarshalText()
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
	t := time.Now()
	err = t.UnmarshalText(a)
	if err != nil {
		return false, errors.New("Comparism error in FieldType date " + typ + " | " + string(a) + " | " + string(b))
	}
	t2 := time.Now()
	err = t2.UnmarshalText(b)
	if err != nil {
		return false, errors.New("Comparism error in FieldType date " + typ + " | " + string(a) + " | " + string(b))
	}

	switch typ {
	case "==":
		return t.Equal(t2), err
	case "!=":
		return !(t.Equal(t2)), err
	case ">":
		return t.After(t2), err
	case ">=":
		return (t.Equal(t2) || t.After(t2)), err
	case "<":
		return t.Before(t2), err
	case "<=":
		return (t.Before(t2) || t.Equal(t2)), err
	default:
		return false, errors.New("fieldtype date does not support this comparison operator " + typ)
	}

}

func (f *FieldTypeDate) CompareIndexed(typ string, a interface{}) (string, string, error) {
	var err error
	t, ok := a.(time.Time)
	if !ok {
		return "", "", errors.New("provided parameter is not of the type time.Time")
	}
	b, err := t.MarshalText()
	if err != nil {
		return "", "", errors.New("provided parameter cant be converted to string in Fieldtype date")
	}
	s := string(b)
	switch typ {
	case "==":
		return s, "==", err
	case ">":
		return s, ">", err
	case ">=":
		return s, ">=", err
	case "<":
		return s, "<", err
	case "<=":
		return s, "<=", err
	default:
		return "", "", errors.New("fieldtype date does not support this comparison operator for indexed field")
	}

}
