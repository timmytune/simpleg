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

func TestNew7(t *testing.T) {
	// should be able to add a key and value
	c := f.New()
	b := c.(float64)
	if b != float64(0) {
		t.Error("FieldTypefloat64.New Failed Test:")
	}
}

func TestGet7(t *testing.T) {
	b, _ := f.Set(1.0000000045)
	if e, _ := f.Get(b); e.(float64) != 1.0000000045 {
		Log.Error().Float64("value", e.(float64)).Msg("just checking")
		t.Error("FieldTypeFloat64.Get Failed Test")
	}
}

func TestSet7(t *testing.T) {
	b, _ := f.Set(189.8978987987897897)
	c, _ := f.Set(189.8978987987897897)
	if !bytes.Equal(b, c) {
		t.Error("FieldTypeInt64.Set Failed Test got:", c)
	}
}

func TestCompare7(t *testing.T) {
	// should be able to add a key and value
	a, _ := f.Set(10.09654)
	b, _ := f.Set(10.09654)
	c, _ := f.Set(11.0965476876)
	e, _ := f.Set(9.09654897879879879)

	if d, _ := f.Compare("==", a, b); d != true {
		t.Error("FieldTypefloat64.Compare.== Failed Test:", d)
	}

	if d, _ := f.Compare("==", b, c); d == true {
		t.Error("FieldTypefloat64.Compare.equal Failed Test:", d)
	}

	if d, _ := f.Compare("!=", a, b); d == true {
		t.Error("FieldTypefloat64.Compare.!= Failed Test:", d)
	}

	if d, _ := f.Compare(">", c, a); d != true {
		t.Error("FieldTypefloat64.Compare.> Failed Test:", d)
	}

	if d, _ := f.Compare(">=", a, c); d == true {
		t.Error("FieldTypefloat64.Compare.>= Failed Test:", d)
	}

	if d, _ := f.Compare("<", a, c); d != true {
		t.Error("FieldTypefloat64.Compare.< Failed Test:", d)
	}

	if d, _ := f.Compare("<=", e, a); d != true {
		t.Error("FieldTypefloat64.Compare.<= Failed Test:", d)
	}

	if _, f := f.Compare("return error", a, b); f == nil {
		t.Error("FieldTypefloat64.Compare.return Failed Test:", f)
	}
}

func TestCompareIndexed7(t *testing.T) {
	g, _ := f.Set(25.0787987)
	if d, f, _ := f.CompareIndexed("==", 25.0787987); d != string(g) || f != "==" {
		t.Error("FieldTypefloat64.CompareIndexed.== Failed Test:", d)
	}

	if d, f, _ := f.CompareIndexed(">", 25.0787987); d != string(g) || f != ">" {
		t.Error("FieldTypefloat64.CompareIndexed.> Failed Test:", d)
	}

	if d, f, _ := f.CompareIndexed(">=", 25.0787987); d != string(g) || f != ">=" {
		t.Error("FieldTypefloat64.CompareIndexed.>= Failed Test:", d)
	}

	if d, f, _ := f.CompareIndexed("<", 25.0787987); d != string(g) || f != "<" {
		t.Error("FieldTypefloat64.CompareIndexed.< Failed Test:", d)
	}

	if _, _, f := f.CompareIndexed("return error", 25.0787987); f == nil {
		t.Error("FieldTypefloat64.CompareIndexed.return Failed Test:", f)
	}
}
