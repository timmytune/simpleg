package simpleg

import (
	"testing"
	"time"
)

func TestNew5(t *testing.T) {
	// should be able to add a key and value
	b := d.New()
	t, ok := b.(time.Time)
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
	t2, err := e.(time.Time)
	if err != nil {
		t.Error("FieldTypeDate.Get Failed Test" + err.Error())
	}
	if !(t1.Equal(t2)) {
		t.Error("FieldTypeDate.Get Failed Test times are not of the same value | " + t1.String() + " | " + t2.String())
	}
}

func TestCompare5(t *testing.T) {
	// should be able to add a key and value
	a := d.Set(int64(10))
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

func TestCompareIndexed5(t *testing.T) {

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
