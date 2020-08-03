package simpleg

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestNew(t *testing.T) {
	// should be able to add a key and value
	if b := s.New(); b != false {
		t.Error("FieldTypeBool.New Failed Test:")
	}
}

func TestGet(t *testing.T) {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint16(b, 1)
	if e := s.Get(b); e != true {
		t.Error("FieldTypeBool.Get Failed Test")
	}
}

func TestSet(t *testing.T) {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint16(b, 0)
	// should be able to add a key and value
	c := s.Set(false)
	if bytes.Compare(b, c) != 0 {
		t.Error("FieldTypeBool.Set Failed Test got:", c)
	}
}

func TestCompare(t *testing.T) {
	// should be able to add a key and value
	a := make([]byte, 4)
	binary.LittleEndian.PutUint16(a, 1)
	b := make([]byte, 4)
	binary.LittleEndian.PutUint16(b, 0)
	d := make([]byte, 4)
	binary.LittleEndian.PutUint16(d, 0)

	if c, _ := s.Compare("==", a, b); c == true {
		t.Error("FieldTypeBool.Compare.equal Failed Test:", c)
	}

	if c, _ := s.Compare("==", b, d); c != true {
		t.Error("FieldTypeBool.Compare.equal Failed Test:", c)
	}

	if d, _ := s.Compare("!=", a, b); d != true {
		t.Error("FieldTypeBool.Compare.notEqual Failed Test:", d)
	}

	if _, f := s.Compare("return error", a, b); f == nil {
		t.Error("FieldTypeBool.Compare.return Failed Test:", f)
	}
}
