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

func TestNew2(t *testing.T) {
	// should be able to add a key and value
	if b := z.New(); b != "" {
		t.Error("FieldTypeString.New Failed Test:")
	}
}

func TestGet2(t *testing.T) {
	b := []byte("new")
	if e, _ := z.Get(b); e != "new" {
		t.Error("FieldTypeString.Get Failed Test")
	}
}

func TestSet2(t *testing.T) {
	b, _ := z.Set("new")
	c, _ := z.Set("new")
	if bytes.Compare(b, c) != 0 {
		t.Error("FieldTypeString.Set Failed Test got:", c)
	}
}

func TestCompare2(t *testing.T) {
	// should be able to add a key and value
	a, _ := z.Set("new")
	b, _ := z.Set("new")
	c, _ := z.Set("new2")
	e, _ := z.Set("NEW")
	f, _ := z.Set("ew")

	if d, _ := z.Compare("==", a, b); d != true {
		t.Error("FieldTypeString.Compare.equal Failed Test:", d)
	}

	if d, _ := z.Compare("==", b, c); d == true {
		t.Error("FieldTypeString.Compare.equal Failed Test:", d)
	}

	if d, _ := z.Compare("!=", a, b); d == true {
		t.Error("FieldTypeString.Compare.notEqual Failed Test:", d)
	}

	if d, _ := z.Compare("contains", c, a); d != true {
		t.Error("FieldTypeString.Compare.contains Failed Test:", d)
	}

	if d, _ := z.Compare("NoCaseEqual", a, e); d != true {
		t.Error("FieldTypeString.Compare.NoCaseEqual Failed Test:", d)
	}

	if d, _ := z.Compare("HasSuffix", a, f); d != true {
		t.Error("FieldTypeString.Compare.notEqual Failed Test:", d)
	}

	if _, f := z.Compare("return error", a, b); f == nil {
		t.Error("FieldTypeString.Compare.return Failed Test:", f)
	}
}
