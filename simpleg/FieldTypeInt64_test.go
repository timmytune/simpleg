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
	b := i.Set(int64(1))
	if e := i.Get(b); e.(int64) != 1 {
		t.Error("FieldTypeInt64.Get Failed Test")
	}
}

func TestSet3(t *testing.T) {
	b := i.Set(int64(1))
	c := i.Set(int64(1))
	if bytes.Compare(b, c) != 0 {
		t.Error("FieldTypeInt64.Set Failed Test got:", c)
	}
}

func TestCompare3(t *testing.T) {
	// should be able to add a key and value
	a := i.Set(int64(10))
	b := i.Set(int64(10))
	c := i.Set(int64(11))
	e := i.Set(int64(9))

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

	if d, f, _ := i.CompareIndexed("==", int64(25)); d != "25" || f != "=" {
		t.Error("FieldTypeInt64.CompareIndexed.== Failed Test:", d)
	}

	if d, f, _ := i.CompareIndexed(">", int64(25)); d != "25" || f != "+" {
		t.Error("FieldTypeInt64.CompareIndexed.> Failed Test:", d)
	}

	if d, f, _ := i.CompareIndexed(">=", int64(25)); d != "25" || f != "+=" {
		t.Error("FieldTypeInt64.CompareIndexed.>= Failed Test:", d)
	}

	if d, f, _ := i.CompareIndexed("<", int64(25)); d != "25" || f != "-" {
		t.Error("FieldTypeInt64.CompareIndexed.< Failed Test:", d)
	}

	if d, f, _ := i.CompareIndexed("<=", int64(25)); d != "25" || f != "-=" {
		t.Error("FieldTypeInt64.CompareIndexed.<= Failed Test:", d)
	}

	if _, _, f := i.CompareIndexed("return error", int64(25)); f == nil {
		t.Error("FieldTypeInt64.CompareIndexed.return Failed Test:", f)
	}
}
