package simpleg

import (
	"errors"
	"time"
)

type FieldTypeDate struct {
}

func (f *FieldTypeDate) GetOption() map[string]string {
	m := make(map[string]string)
	m["Name"] = "date"
	m["AllowIndexing"] = "1"
	return m
}

func (f *FieldTypeDate) New() interface{} {
	return time.Now()
}

func (f *FieldTypeDate) Set(v interface{}) ([]byte, error) {
	d, ok := v.(time.Time)
	if !ok {
		return nil, errors.New("Provided interface is not of type Time")
	}
	t, err := d.MarshalJSON()
	return t, err
}

func (f *FieldTypeDate) Get(v []byte) (interface{}, error) {
	t := time.Now()
	err := t.UnmarshalText(v)
	if err != nil {
		return t, err
	}
	return t, err
}

func (f *FieldTypeDate) Compare(typ string, a []byte, b []byte) (bool, error) {
	var err error
	t := time.Now()
	err = t.UnmarshalText(a)
	if err != nil {
		return false, errors.New("Comparism error in FieldType date " + typ + " | " + string(a) + " | " + string(b))
	}
	t2 := time.Now()
	err = t2.UnmarshalText(b)
	if err != nil {
		return false, errors.New("Comparism error in FieldType date " + typ + " | " + string(a) + " | " + string(b))
	}

	switch typ {
	case "==":
		return t.Equal(t2), err
	case "!=":
		return !(t.Equal(t2)), err
	case ">":
		return t.After(t2), err
	case ">=":
		return (t.Equal(t2) || t.After(t2)), err
	case "<":
		return t.Before(t2), err
	case "<=":
		return (t.Before(t2) || t.Equal(t2)), err
	default:
		return false, errors.New("fieldtype date does not support this comparison operator " + typ)
	}

}

func (f *FieldTypeDate) CompareIndexed(typ string, a interface{}) (string, string, error) {
	var err error
	t, ok := a.(time.Time)
	if !ok {
		return "", "", errors.New("Provided parameter is not of the type time.Time")
	}
	b, err := t.MarshalText()
	if err != nil {
		return "", "", errors.New("Provided parameter cant be converted to string in Fieldtype date")
	}
	s := string(b)
	switch typ {
	case "==":
		return s, "=", err
	case ">":
		return s, "+", err
	case ">=":
		return s, "+=", err
	case "<":
		return s, "-", err
	case "<=":
		return s, "-=", err
	default:
		return "", "", errors.New("fieldtype date does not support this comparison operator for indexed field")
	}

}
