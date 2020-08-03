package simpleg

import (
	"errors"
	"strconv"
	"strings"

	"github.com/badoux/checkmail"
)

type FieldValidation struct {
}

//StringName is validation for strings that will be names
func (f *FieldValidation) String(name string, low int, high int, transformLower bool, isLetters bool, isUsername bool) func(interface{}, *DB) (bool, interface{}, error) {

	IsLetter := func(s string) bool {
		for _, r := range s {
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
				return false
			}
		}
		return true
	}

	IsUsename := func(s string) bool {
		for _, r := range s {
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
				return false
			}
		}
		return true
	}

	return func(v interface{}, db *DB) (bool, interface{}, error) {
		d, e := v.(string)
		if e != true {
			return false, v, errors.New("the provided argument for " + name + " is not a string")
		}
		l := len([]rune(d))
		if l < low && low != -1 {
			return false, v, errors.New(name + " is less than " + strconv.Itoa(low) + " characters")
		}
		if l > high && high != -1 {
			return false, v, errors.New(name + " is more than " + strconv.Itoa(high) + " characters")
		}
		if isLetters {
			if IsLetter(d) == false {
				return false, v, errors.New(name + " can only contain alphanumeric characters")
			}
		}
		if isUsername {
			if IsUsename(d) == false {
				return false, v, errors.New(name + " can only contain alphanumeric characters and numbers")
			}
		}
		if transformLower {
			return true, strings.ToLower(d), nil
		}

		return true, d, nil
	}
}

func (f *FieldValidation) Email(name string, transformLower bool) func(interface{}, *DB) (bool, interface{}, error) {

	return func(v interface{}, db *DB) (bool, interface{}, error) {
		d, e := v.(string)
		if e != true {
			return false, v, errors.New("the provided argument for " + name + " is not a string")
		}
		err := checkmail.ValidateFormat(d)
		if err != nil {
			return false, v, errors.New("Invalid email format for field " + name)
		}
		// err2 := checkmail.ValidateHost(d)
		// if err2 != nil {
		// 	return false, v, err2
		// }

		if transformLower {
			return true, strings.ToLower(d), nil
		}

		return true, d, nil
	}
}

func (f *FieldValidation) Int64(name string, low int, high int) func(interface{}, *DB) (bool, interface{}, error) {

	return func(v interface{}, db *DB) (bool, interface{}, error) {
		d, e := v.(int64)
		if e != true {
			return false, v, errors.New("the provided argument for " + name + " is not of type int64")
		}
		if d < int64(low) && low != -1 {
			return false, v, errors.New(strconv.Itoa(int(d)) + " is less than the minimum of " + strconv.Itoa(int(low)) + " for field " + name)
		}
		if d > int64(high) && high != -1 {
			return false, v, errors.New(strconv.Itoa(int(d)) + " is greater than the maximum of " + strconv.Itoa(int(high)) + " for field " + name)
		}
		return true, d, nil
	}
}
