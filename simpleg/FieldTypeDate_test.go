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
	"testing"
	"time"
)

func TestNew5(t *testing.T) {
	// should be able to add a key and value
	b := d.New()
	_, ok := b.(time.Time)
	if !ok {
		t.Error("FieldTypeDate.New Failed Test:")
	}
}

func TestGet5(t *testing.T) {
	t1 := time.Now()
	b, err := d.Set(t1)
	if err != nil {
		t.Error("FieldTypeDate.Get Failed Test" + err.Error())
	}
	e, err := d.Get(b)
	if err != nil {
		t.Error("FieldTypeDate.Get Failed Test" + err.Error())
	}
	t2, bul := e.(time.Time)
	if !bul {
		t.Error("FieldTypeDate.Get Failed Test" + err.Error())
	}
	if !(t1.Equal(t2)) {
		t.Error("FieldTypeDate.Get Failed Test times are not of the same value | " + t1.String() + " | " + t2.String())
	}
}

func TestCompare5(t *testing.T) {
	// should be able to add a key and value
	t2 := time.Now()
	a, _ := d.Set(t2)
	b, _ := d.Set(t2)
	c, _ := d.Set(time.Unix(int64(1598456600), int64(0)))
	e, _ := d.Set(time.Unix(int64(1598456699), int64(0)))

	if d2, _ := d.Compare("==", a, b); d2 != true {
		t.Error("FieldTypeDate.Compare.== Failed Test:", d2)
	}

	if d2, _ := d.Compare("==", b, c); d2 == true {
		t.Error("FieldTypeDate.Compare.equal Failed Test:", d2)
	}

	if d2, _ := d.Compare("!=", a, b); d2 == true {
		t.Error("FieldTypeDate.Compare.!= Failed Test:", d2)
	}

	if d2, _ := d.Compare(">", a, c); d2 != true {
		t.Error("FieldTypeDate.Compare.> Failed Test:", c, a, d2)
	}

	if d2, _ := d.Compare(">=", a, c); d2 != true {
		t.Error("FieldTypeDate.Compare.>= Failed Test:", d2)
	}

	if d2, _ := d.Compare("<", c, a); d2 != true {
		t.Error("FieldTypeDate.Compare.< Failed Test:", d2)
	}

	if d2, _ := d.Compare("<=", e, a); d2 != true {
		t.Error("FieldTypeDate.Compare.<= Failed Test:", d2)
	}

	if _, f := d.Compare("return error", a, b); f == nil {
		t.Error("FieldTypeDate.Compare.return Failed Test:", f)
	}
}

func TestCompareIndexed5(t *testing.T) {

}
