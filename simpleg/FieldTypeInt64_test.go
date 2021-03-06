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
	"testing"
)

func TestNew3(t *testing.T) {
	// should be able to add a key and value
	if b := i.New(); b != int64(0) {
		t.Error("FieldTypeInt64.New Failed Test:")
	}
}

func TestGet3(t *testing.T) {
	b, _ := i.Set(int64(1))
	if e, _ := i.Get(b); e.(int64) != 1 {
		t.Error("FieldTypeInt64.Get Failed Test")
	}
}

func TestSet3(t *testing.T) {
	b, _ := i.Set(int64(1))
	c, _ := i.Set(int64(1))
	if bytes.Compare(b, c) != 0 {
		t.Error("FieldTypeInt64.Set Failed Test got:", c)
	}
}

func TestCompare3(t *testing.T) {
	// should be able to add a key and value
	a, _ := i.Set(int64(10))
	b, _ := i.Set(int64(10))
	c, _ := i.Set(int64(11))
	e, _ := i.Set(int64(9))

	if d, _ := i.Compare("==", a, b); d != true {
		t.Error("FieldTypeInt64.Compare.== Failed Test:", d)
	}

	if d, _ := i.Compare("==", b, c); d == true {
		t.Error("FieldTypeInt64.Compare.equal Failed Test:", d)
	}

	if d, _ := i.Compare("!=", a, b); d == true {
		t.Error("FieldTypeInt64.Compare.!= Failed Test:", d)
	}

	if d, _ := i.Compare(">", c, a); d != true {
		t.Error("FieldTypeInt64.Compare.> Failed Test:", d)
	}

	if d, _ := i.Compare(">=", a, c); d == true {
		t.Error("FieldTypeInt64.Compare.>= Failed Test:", d)
	}

	if d, _ := i.Compare("<", a, c); d != true {
		t.Error("FieldTypeInt64.Compare.< Failed Test:", d)
	}

	if d, _ := i.Compare("<=", e, a); d != true {
		t.Error("FieldTypeInt64.Compare.<= Failed Test:", d)
	}

	if _, f := i.Compare("return error", a, b); f == nil {
		t.Error("FieldTypeInt64.Compare.return Failed Test:", f)
	}
}

func TestCompareIndexed3(t *testing.T) {
	g, _ := i.Set(int64(25))
	if d, f, _ := i.CompareIndexed("==", int64(25)); d != string(g) || f != "==" {
		t.Error("FieldTypeInt64.CompareIndexed.== Failed Test:", d)
	}

	if d, f, _ := i.CompareIndexed(">", int64(25)); d != string(g) || f != ">" {
		t.Error("FieldTypeInt64.CompareIndexed.> Failed Test:", d)
	}

	if d, f, _ := i.CompareIndexed(">=", int64(25)); d != string(g) || f != ">=" {
		t.Error("FieldTypeInt64.CompareIndexed.>= Failed Test:", d)
	}

	if d, f, _ := i.CompareIndexed("<", int64(25)); d != string(g) || f != "<" {
		t.Error("FieldTypeInt64.CompareIndexed.< Failed Test:", d)
	}

	if _, _, f := i.CompareIndexed("return error", int64(25)); f == nil {
		t.Error("FieldTypeInt64.CompareIndexed.return Failed Test:", f)
	}
}
