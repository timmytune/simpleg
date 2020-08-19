package simpleg

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

var s FieldTypeBool
var z FieldTypeString
var i FieldTypeInt64
var u FieldTypeUint64
var d FieldTypeDate
var db *DB

type User struct {
	db        *DB
	ID        uint64
	firstName string
	lastName  string
	email     string
	active    bool
	age       int64
}

type Friend struct {
	db       *DB
	FROM     uint64
	TO       uint64
	accepted bool
}

func GetUserOption() ObjectTypeOptions {
	uo := ObjectTypeOptions{}
	uo.Name = "User"
	uo.New = func(db *DB) interface{} {
		return User{db: db, active: true}
	}
	uo.Get = func(m map[KeyValueKey][]byte, db *DB) (interface{}, []error) {
		e := make([]error, 0)
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered in Get User Object type", r)
				switch x := r.(type) {
				case string:
					err := errors.New(x)
					e = append(e, err)
				case error:
					err := x
					e = append(e, err)
				default:
					e = append(e, errors.New("unknown panic"))
				}
			}
		}()
		db.Lock.Lock()
		defer db.Lock.Unlock()
		u := User{db: db}

		if id, ok := m[KeyValueKey{Main: "ID"}]; ok {
			u.ID = db.FT["uint64"].Get(id).(uint64)
		} else {
			e = append(e, errors.New("The Data from the DB has no ID"))
			return nil, e
		}
		if f, ok := m[KeyValueKey{Main: "firstName"}]; ok {
			u.firstName = db.FT["string"].Get(f).(string)
		}
		if f, ok := m[KeyValueKey{Main: "lastName"}]; ok {
			u.lastName = db.FT["string"].Get(f).(string)
		}
		if f, ok := m[KeyValueKey{Main: "email"}]; ok {
			u.email = db.FT["string"].Get(f).(string)
		}
		if f, ok := m[KeyValueKey{Main: "active"}]; ok {
			u.active = db.FT["bool"].Get(f).(bool)
		}
		if f, ok := m[KeyValueKey{Main: "age"}]; ok {
			u.age = db.FT["int64"].Get(f).(int64)
		}
		return u, e
	}
	uo.Set = func(i interface{}, db *DB) (u map[KeyValueKey][]byte, e []error) {
		u = make(map[KeyValueKey][]byte)
		e = make([]error, 0)
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered in Get User", r)
				switch x := r.(type) {
				case string:
					err := errors.New(x)
					e = append(e, err)
				case error:
					err := x
					e = append(e, err)
				default:
					e = append(e, errors.New("unknown panic"))
				}
			}
		}()
		d := i.(User)
		db.Lock.Lock()
		defer db.Lock.Unlock()
		if d.ID > 0 {
			u[KeyValueKey{Main: "ID"}] = db.FT["uint64"].Set(d.ID)
		}
		if d.firstName != "" {
			u[KeyValueKey{Main: "firstName"}] = db.FT["string"].Set(d.firstName)
		}
		if d.lastName != "" {
			u[KeyValueKey{Main: "lastName"}] = db.FT["string"].Set(d.lastName)
		}
		if d.email != "" {
			u[KeyValueKey{Main: "email"}] = db.FT["string"].Set(d.email)
		}
		u[KeyValueKey{Main: "active"}] = db.FT["bool"].Set(d.active)
		if d.age > 0 {
			u[KeyValueKey{Main: "age"}] = db.FT["int64"].Set(d.age)
		}
		return
	}
	uo.Validate = func(i interface{}, db *DB) (interface{}, []error) {
		e := make([]error, 0)
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered in Validate User", r)
				switch x := r.(type) {
				case string:
					err := errors.New(x)
					e = append(e, err)
				case error:
					err := x
					e = append(e, err)
				default:
					e = append(e, errors.New("unknown panic"))
				}
			}
		}()
		db.Lock.Lock()
		defer db.Lock.Unlock()
		d := i.(User)
		x, y, z := db.OT["User"].Fields["firstName"].Validate(d.firstName, db)
		if !x {
			e = append(e, z)
		} else {
			d.firstName = y.(string)
		}
		x, y, z = db.OT["User"].Fields["lastName"].Validate(d.lastName, db)

		if !x {
			e = append(e, z)
		} else {
			d.lastName = y.(string)

		}
		x, y, z = db.OT["User"].Fields["email"].Validate(d.email, db)
		if !x {
			e = append(e, z)
		} else {
			d.email = y.(string)
		}
		x, y, z = db.OT["User"].Fields["age"].Validate(d.age, db)
		if !x {
			e = append(e, z)
		}

		return d, e
	}
	fv := FieldValidation{}
	uo.Fields = make(map[string]FieldOptions)
	uo.Fields["firstName"] = FieldOptions{Indexed: true, FieldType: "string", Validate: fv.String("firstName", 3, 20, true, true, false)}
	uo.Fields["lastName"] = FieldOptions{Indexed: true, FieldType: "string", Validate: fv.String("lastName", 3, 20, true, true, false)}
	uo.Fields["email"] = FieldOptions{Indexed: true, FieldType: "string", Validate: fv.Email("email", true)}
	uo.Fields["active"] = FieldOptions{Indexed: false, FieldType: "bool", Validate: nil}
	uo.Fields["age"] = FieldOptions{Indexed: true, FieldType: "int64", Validate: fv.Int64("age", 18, -1)}

	return uo
}

func GetFriendLinkOption() LinkTypeOptions {
	fl := LinkTypeOptions{}
	fl.OppositeSame = true
	fl.Name = "Friend"
	fl.From = "User"
	fl.To = "User"
	fl.New = func(db *DB) interface{} {
		return Friend{db: db}
	}
	fl.Get = func(m map[KeyValueKey][]byte, db *DB) (interface{}, []error) {
		e := make([]error, 0)
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered in Get User Object type", r)
				switch x := r.(type) {
				case string:
					err := errors.New(x)
					e = append(e, err)
				case error:
					err := x
					e = append(e, err)
				default:
					e = append(e, errors.New("unknown panic"))
				}
			}
		}()
		db.Lock.Lock()
		defer db.Lock.Unlock()
		friend := Friend{db: db}
		if f, ok := m[KeyValueKey{Main: "FROM"}]; ok {
			friend.FROM = db.FT["uint64"].Get(f).(uint64)
		} else {
			e = append(e, errors.New("The Data from the DB has no FROM field set"))
			return nil, e
		}
		if f, ok := m[KeyValueKey{Main: "TO"}]; ok {
			friend.TO = db.FT["uint64"].Get(f).(uint64)
		} else {
			e = append(e, errors.New("The Data from the DB has no TO field set"))
			return nil, e
		}
		if f, ok := m[KeyValueKey{Main: "accepted"}]; ok {
			friend.accepted = db.FT["bool"].Get(f).(bool)
		}
		return u, e
	}
	fl.Set = func(i interface{}, db *DB) (u map[KeyValueKey][]byte, e []error) {
		u = make(map[KeyValueKey][]byte)
		e = make([]error, 0)
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered in Get User", r)
				switch x := r.(type) {
				case string:
					err := errors.New(x)
					e = append(e, err)
				case error:
					err := x
					e = append(e, err)
				default:
					e = append(e, errors.New("unknown panic"))
				}
			}
		}()
		d, ok := i.(Friend)
		if !ok {
			e = append(e, errors.New("The Provided struct is not of type Friend"))
			return nil, e
		}
		db.Lock.Lock()
		defer db.Lock.Unlock()
		if d.FROM > 0 {
			u[KeyValueKey{Main: "FROM"}] = db.FT["uint64"].Set(d.FROM)
		} else {
			e = append(e, errors.New("From Field not provided"))
		}
		if d.TO > 0 {
			u[KeyValueKey{Main: "TO"}] = db.FT["uint64"].Set(d.TO)
		} else {
			e = append(e, errors.New("TO Field not provided"))
		}
		u[KeyValueKey{Main: "accepted"}] = db.FT["bool"].Set(d.accepted)
		return
	}
	fl.Validate = func(i interface{}, db *DB) (interface{}, []error) {
		e := make([]error, 0)
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered in Validate User", r)
				switch x := r.(type) {
				case string:
					err := errors.New(x)
					e = append(e, err)
				case error:
					err := x
					e = append(e, err)
				default:
					e = append(e, errors.New("unknown panic"))
				}
			}
		}()
		return i, e
	}
	//fv := FieldValidation{}
	fl.Fields = make(map[string]FieldOptions)
	fl.Fields["accepted"] = FieldOptions{FieldType: "bool"}

	return fl
}

func TestMain(m *testing.M) {

	s = FieldTypeBool{}
	z = FieldTypeString{}
	i = FieldTypeInt64{}
	u = FieldTypeUint64{}
	d = FieldTypeDate{}

	opt := DefaultOptions()
	db = &DB{}
	db.Init(opt)
	db.AddObjectType(GetUserOption())
	db.AddLinkType(GetFriendLinkOption())
	db.Start()

	ret := m.Run()

	db.Close()
	os.RemoveAll(opt.DataDirectory)

	os.Exit(ret)

}

func TestSetObject(t *testing.T) {
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ret := db.Get("object.new", "User")
			if len(ret.Errors) > 0 {
				t.Error("simpleg.Set Failed Test get user:", ret)
			}
			u, _ := ret.Data.(User)
			u.firstName = "Yinka"
			u.lastName = "Adedoyin"
			u.email = "timmytune002@gmail.com"
			u.age = int64(19)
			re := db.Set("save.object", "User", u)
			if len(re.Errors) != 0 {
				t.Error("simpleg.Set Failed Test got:", ret)
			}
		}()
	}
	wg.Wait()
	//data, _ := db.KV.Stream([]string{"simpleg"}, nil)
	//log.Print("Data in the database", data)
	elapsed := time.Since(start)
	log.Printf("-------------------------------------------- Test Set Object took %s", elapsed)

}

func TestSetters(t *testing.T) {
	start := time.Now()

	re := db.Get("object.new", "User")
	if len(re.Errors) > 0 {
		t.Error("simpleg.Set Failed Test get user:", re)
	}
	u, _ := re.Data.(User)
	u.firstName = "Femi"
	u.lastName = "Adedoyin"
	u.email = "timmytune001@gmail.com"
	u.age = int64(31)
	ret := db.Set("save.object", "User", u)
	if len(ret.Errors) != 0 {
		t.Error("simpleg.Set Failed Test got:", ret)
	}

	re = db.Get("object.new", "User")
	if len(re.Errors) > 0 {
		t.Error("simpleg.Set Failed Test get user:", re)
	}
	uu, _ := re.Data.(User)
	uu.firstName = "Fe"
	uu.lastName = "Ad"
	uu.email = "timmytune0gmail.com"
	uu.age = int64(12)
	ret = db.Set("save.object", "User", uu)
	if len(ret.Errors) == 0 {
		t.Error("simpleg.Set Failed Test got:", ret)
	}
	fmt.Println("The errors that where thrown ", ret)

	ret = db.Set("save.object.field", "User", ret.ID, "lastName", "Adebara")

	if len(ret.Errors) != 0 {
		t.Error("simpleg.Update Failed Test got:", ret)
	}
	elapsed := time.Since(start)

	log.Printf("-------------------------------------------- Test Setter took %s", elapsed)
	time.Sleep(1 * time.Second)
	//data, _ := db.KV.Stream([]string{"simpleg"}, nil) //, "^", "User", "^", string(ret.ID)
	//log.Print("Data in the database", data)
}

func TestGetObject(t *testing.T) {
	//var wg sync.WaitGroup
	start := time.Now()
	for i := 1; i < 5; i++ {
		//wg.Add(1)
		//go func() {
		//defer wg.Done()
		ret := db.Get("object.single", "User", uint64(i))
		if len(ret.Errors) > 0 {
			t.Error("simpleg.Set Failed Test get user:", ret)
		}
		u, _ := ret.Data.(User)
		log.Print("-------------------------------------------- Test Get Object", u)

		//}()
	}
	//wg.Wait()
	elapsed := time.Since(start)
	log.Printf("-------------------------------------------- Test Set Object took %s", elapsed)

}
