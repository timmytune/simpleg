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
	b, _ := i.Set(t2)
	c, _ := d.Set(time.Now())
	e, _ := d.Set(time.Now())

	if d2, _ := d.Compare("==", a, b); d2 != true {
		t.Error("FieldTypeDate.Compare.== Failed Test:", d2)
	}

	if d2, _ := d.Compare("==", b, c); d2 == true {
		t.Error("FieldTypeDate.Compare.equal Failed Test:", d2)
	}

	if d2, _ := d.Compare("!=", a, b); d2 == true {
		t.Error("FieldTypeDate.Compare.!= Failed Test:", d2)
	}

	if d2, _ := d.Compare(">", c, a); d2 != true {
		t.Error("FieldTypeDate.Compare.> Failed Test:", d2)
	}

	if d2, _ := d.Compare(">=", a, c); d2 == true {
		t.Error("FieldTypeDate.Compare.>= Failed Test:", d2)
	}

	if d2, _ := d.Compare("<", a, c); d2 != true {
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

	if d, f, _ := i.CompareIndexed("==", int64(25)); d != "25" || f != "=" {
		t.Error("FieldTypeDate.CompareIndexed.== Failed Test:", d)
	}

	if d, f, _ := i.CompareIndexed(">", int64(25)); d != "25" || f != "+" {
		t.Error("FieldTypeDate.CompareIndexed.> Failed Test:", d)
	}

	if d, f, _ := i.CompareIndexed(">=", int64(25)); d != "25" || f != "+=" {
		t.Error("FieldTypeDate.CompareIndexed.>= Failed Test:", d)
	}

	if d, f, _ := i.CompareIndexed("<", int64(25)); d != "25" || f != "-" {
		t.Error("FieldTypeDate.CompareIndexed.< Failed Test:", d)
	}

	if d, f, _ := i.CompareIndexed("<=", int64(25)); d != "25" || f != "-=" {
		t.Error("FieldTypeDate.CompareIndexed.<= Failed Test:", d)
	}

	if _, _, f := i.CompareIndexed("return error", int64(25)); f == nil {
		t.Error("FieldTypeDate.CompareIndexed.return Failed Test:", f)
	}
}
