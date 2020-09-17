package simpleg

import (
	"errors"
	"fmt"
	"os"
	debug "runtime/debug"
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
	CREATED  time.Time
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
		db.RLock()
		defer db.RUnlock()
		u := User{db: db}

		if id, ok := m[KeyValueKey{Main: "ID"}]; ok {
			f, err := db.FT["uint64"].Get(id)
			if err != nil {
				e = append(e, err)
				return nil, e
			}
			u.ID = f.(uint64)
		} else {
			e = append(e, errors.New("The Data from the DB has no ID"))
			return nil, e
		}
		if f, ok := m[KeyValueKey{Main: "firstName"}]; ok {
			v, err := db.FT["string"].Get(f)
			if err != nil {
				e = append(e, err)
			} else {
				u.firstName = v.(string)
			}
		}
		if f, ok := m[KeyValueKey{Main: "lastName"}]; ok {
			v, err := db.FT["string"].Get(f)
			if err != nil {
				e = append(e, err)
			} else {
				u.lastName = v.(string)
			}
		}
		if f, ok := m[KeyValueKey{Main: "email"}]; ok {
			v, err := db.FT["string"].Get(f)
			if err != nil {
				e = append(e, err)
			} else {
				u.email = v.(string)
			}
		}
		if f, ok := m[KeyValueKey{Main: "active"}]; ok {
			v, err := db.FT["bool"].Get(f)
			if err != nil {
				e = append(e, err)
			} else {
				u.active = v.(bool)
			}
		}
		if f, ok := m[KeyValueKey{Main: "age"}]; ok {
			v, err := db.FT["int64"].Get(f)
			if err != nil {
				e = append(e, err)
			} else {
				u.age = v.(int64)
			}
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
		db.RLock()
		defer db.RUnlock()
		if d.ID > 0 {
			u[KeyValueKey{Main: "ID"}], _ = db.FT["uint64"].Set(d.ID)
		}
		if d.firstName != "" {
			u[KeyValueKey{Main: "firstName"}], _ = db.FT["string"].Set(d.firstName)
		}
		if d.lastName != "" {
			u[KeyValueKey{Main: "lastName"}], _ = db.FT["string"].Set(d.lastName)
		}
		if d.email != "" {
			u[KeyValueKey{Main: "email"}], _ = db.FT["string"].Set(d.email)
		}
		u[KeyValueKey{Main: "active"}], _ = db.FT["bool"].Set(d.active)
		if d.age > 0 {
			u[KeyValueKey{Main: "age"}], _ = db.FT["int64"].Set(d.age)
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
		db.RLock()
		defer db.RUnlock()
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
	uo.Fields["age"] = FieldOptions{Indexed: true, FieldType: "int64", Validate: fv.Int64("age", 10, -1)}

	return uo
}

func GetFriendLinkOption() LinkTypeOptions {
	fl := LinkTypeOptions{}
	fl.OppositeSame = true
	fl.Multiple = false
	fl.Name = "Friend"
	fl.From = "User"
	fl.To = "User"
	fl.New = func(db *DB) interface{} {
		return Friend{db: db, CREATED: time.Now()}
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
		db.RLock()
		defer db.RUnlock()
		friend := Friend{db: db}
		if f, ok := m[KeyValueKey{Main: "FROM"}]; ok {
			v, err := db.FT["uint64"].Get(f)
			if err != nil {
				e = append(e, err)
			} else {
				friend.FROM = v.(uint64)
			}
		} else {
			e = append(e, errors.New("The Data from the DB has no FROM field set"))
			return nil, e
		}
		if f, ok := m[KeyValueKey{Main: "TO"}]; ok {
			v, err := db.FT["uint64"].Get(f)
			if err != nil {
				e = append(e, err)
			} else {
				friend.TO = v.(uint64)
			}
		} else {
			e = append(e, errors.New("The Data from the DB has no TO field set"))
			return nil, e
		}
		if f, ok := m[KeyValueKey{Main: "accepted"}]; ok {
			v, err := db.FT["bool"].Get(f)
			if err != nil {
				e = append(e, err)
			} else {
				friend.accepted = v.(bool)
			}
		}
		if f, ok := m[KeyValueKey{Main: "CREATED"}]; ok {
			v, err := db.FT["date"].Get(f)
			if err != nil {
				e = append(e, err)
			} else {
				friend.CREATED = v.(time.Time)
			}
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
		db.RLock()
		defer db.RUnlock()
		if d.FROM > 0 {
			u[KeyValueKey{Main: "FROM"}], _ = db.FT["uint64"].Set(d.FROM)
		} else {
			e = append(e, errors.New("From Field not provided"))
		}
		if d.TO > 0 {
			u[KeyValueKey{Main: "TO"}], _ = db.FT["uint64"].Set(d.TO)
		} else {
			e = append(e, errors.New("TO Field not provided"))
		}
		u[KeyValueKey{Main: "accepted"}], _ = db.FT["bool"].Set(d.accepted)
		u[KeyValueKey{Main: "CREATED"}], _ = db.FT["date"].Set(d.CREATED)
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
	fl.Fields["CREATED"] = FieldOptions{FieldType: "date"}

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
	elapsed := time.Since(start)
	Log.Printf("-------------------------------------------- Test Set Object took %s", elapsed)

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
	u.age = int64(12)
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

	Log.Printf("-------------------------------------------- Test Setter took %s", elapsed)
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
		Log.Print("-------------------------------------------- Test Get Object", u)

		//}()
	}
	//wg.Wait()
	elapsed := time.Since(start)
	Log.Printf("-------------------------------------------- Test Set Object took %s", elapsed)

}

func TestGetObjects(t *testing.T) {
	//var wg sync.WaitGroup
	//time.Sleep(3 * time.Second)
	start := time.Now()
	q := db.Query()
	n := NodeQuery{}
	n.Name("da").Object("User").Q("age", ">=", int64(12)).Order("ID", "dsc")
	q.Do("object", n)
	ret := q.Return("array", "da")
	if len(ret.Errors) > 0 {
		t.Error("simpleg.Query Failed Test get users:", ret.Errors)
	}
	//u, _ := ret.Data.([]User)
	Log.Print("-------------------------------------------- Test Get Objects", ret)
	//wg.Wait()
	elapsed := time.Since(start)
	Log.Printf("-------------------------------------------- Test Set Objects took %s", elapsed)
}

func TestSetLink(t *testing.T) {
	time.Sleep(1 * time.Second)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ret := db.Get("link.new", "Friend")
			if len(ret.Errors) > 0 {
				t.Error("simpleg.Set Failed Test get Friend:", ret)
			}
			//Log.Print("Get Friend _________________________-", ret.Data)
			f, ok := ret.Data.(Friend)
			if !ok {
				t.Error("simpleg.SetLink Failed Test get user:", ret)
			}
			f.FROM = uint64(1)
			f.TO = uint64(2)
			f.accepted = true
			r := db.Set("save.link", "Friend", f)
			if len(r.Errors) > 0 {
				t.Error("simpleg.Set Failed Test set Friend:", r)
			}
			r = db.Set("save.link.field", "Friend", uint64(1), uint64(2), "accepted", false)
			if len(r.Errors) > 0 {
				Log.Printf("-------------------------------------------- Test Set Object took %s", debug.Stack())
				t.Error("simpleg.Set Failed Test set Friend:", r)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	Log.Printf("-------------------------------------------- Test Set Object took %s", elapsed)

}
