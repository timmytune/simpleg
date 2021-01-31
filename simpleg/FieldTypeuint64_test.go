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

func TestNew4(t *testing.T) {
	// should be able to add a key and value
	if b := u.New(); b != uint64(0) {
		t.Error("FieldTypeUint64.New Failed Test:")
	}
}

func TestGet4(t *testing.T) {
	b, _ := u.Set(uint64(1))
	if e, _ := u.Get(b); e.(uint64) != 1 {
		t.Error("FieldTypeUint64.Get Failed Test")
	}
}

func TestSet4(t *testing.T) {
	b, _ := u.Set(uint64(1))
	c, _ := u.Set(uint64(1))
	if bytes.Compare(b, c) != 0 {
		t.Error("FieldTypeUint64.Set Failed Test got:", c)
	}
}

func TestCompare4(t *testing.T) {
	// should be able to add a key and value
	a, _ := u.Set(uint64(10))
	b, _ := u.Set(uint64(10))
	c, _ := u.Set(uint64(11))
	e, _ := u.Set(uint64(9))

	if d, _ := u.Compare("==", a, b); d != true {
		t.Error("FieldTypeUint64.Compare.== Failed Test:", d)
	}

	if d, _ := u.Compare("==", b, c); d == true {
		t.Error("FieldTypeUint64.Compare.equal Failed Test:", d)
	}

	if d, _ := u.Compare("!=", a, b); d == true {
		t.Error("FieldTypeUint64.Compare.!= Failed Test:", d)
	}

	if d, _ := u.Compare(">", c, a); d != true {
		t.Error("FieldTypeUint64.Compare.> Failed Test:", d)
	}

	if d, _ := u.Compare(">=", a, c); d == true {
		t.Error("FieldTypeUint64.Compare.>= Failed Test:", d)
	}

	if d, _ := u.Compare("<", a, c); d != true {
		t.Error("FieldTypeUint64.Compare.< Failed Test:", d)
	}

	if d, _ := u.Compare("<=", e, a); d != true {
		t.Error("FieldTypeUint64.Compare.<= Failed Test:", d)
	}

	if _, f := u.Compare("return error", a, b); f == nil {
		t.Error("FieldTypeUint64.Compare.return Failed Test:", f)
	}
}

func TestCompareIndexed4(t *testing.T) {
	g, _ := u.Set(uint64(25))
	if d, f, _ := u.CompareIndexed("==", uint64(25)); d != string(g) || f != "==" {
		t.Error("FieldTypeInt64.CompareIndexed.== Failed Test:", d)
	}

	if d, f, _ := u.CompareIndexed(">", uint64(25)); d != string(g) || f != ">" {
		t.Error("FieldTypeInt64.CompareIndexed.> Failed Test:", d)
	}

	if d, f, _ := u.CompareIndexed(">=", uint64(25)); d != string(g) || f != ">=" {
		t.Error("FieldTypeInt64.CompareIndexed.>= Failed Test:", d)
	}

	if d, f, _ := u.CompareIndexed("<", uint64(25)); d != string(g) || f != "<" {
		t.Error("FieldTypeInt64.CompareIndexed.< Failed Test:", d)
	}

	if _, _, f := u.CompareIndexed("return error", uint64(25)); f == nil {
		t.Error("FieldTypeInt64.CompareIndexed.return Failed Test:", f)
	}
}
