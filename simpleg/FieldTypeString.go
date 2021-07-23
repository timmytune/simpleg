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
	"errors"
	"regexp"
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

func (f *FieldTypeString) Set(v interface{}) ([]byte, error) {
	d, ok := v.(string)
	if !ok {
		return nil, errors.New("interface is not the type of string")
	}
	return []byte(d), nil
}

func (f *FieldTypeString) Get(v []byte) (interface{}, error) {
	return string(v), nil
}

func (f *FieldTypeString) Compare(typ string, a []byte, b []byte) (bool, error) {
	var err error

	switch typ {
	case "==":
		return bytes.Equal(a, b), err
	case "!=":
		return !bytes.Equal(a, b), err
	case "contains":
		return bytes.Contains(a, b), err
	case "NoCaseEqual":
		return bytes.EqualFold(a, b), err
	case "HasSuffix":
		return bytes.HasSuffix(a, b), err
	case "prefix":
		return bytes.HasPrefix(a, b), err
	case "regex":
		r, err := regexp.Compile(string(b))
		if err != nil {
			return false, err
		}
		return r.MatchString(string(a)), nil
	default:
		return false, errors.New("fieldtype string does not support the comparison operator " + typ)
	}

}

func (f *FieldTypeString) CompareIndexed(typ string, a interface{}) (string, string, error) {
	var err error
	b, _ := a.(string)
	switch typ {
	case "prefix":
		return b, "perfix", err
	case "==":
		return b, "==", err
	default:
		return "", "", errors.New("fieldtype string does not support this comparison operator for indexed field")
	}

}
