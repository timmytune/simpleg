package simpleg

import (
	"errors"
	"fmt"
	"log"
	"time"
)

//object
type User struct {
	DB        *DB
	ID        uint64
	firstName string
	lastName  string
	email     string
	active    bool
	age       int64
}

//object
type Post struct {
	DB   *DB
	ID   uint64
	body string
}

//Link
type Friend struct {
	DB       *DB
	FROM     uint64
	TO       uint64
	created  time.Time
	accepted bool
}

//Link
type Author struct {
	DB   *DB
	FROM uint64
	TO   uint64
}

//Link
type Like struct {
	DB   *DB
	FROM uint64
	TO   uint64
}

func GetUserOption() ObjectTypeOptions {
	uo := ObjectTypeOptions{}
	uo.Name = "User"
	uo.New = func(db *DB) interface{} {
		return User{DB: db, active: true}
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
		u := User{DB: db}

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
	uo.Fields["age"] = FieldOptions{Indexed: true, FieldType: "int64", Validate: fv.Int64("age", 10, 28)}

	return uo
}

func GetPostOption() ObjectTypeOptions {
	po := ObjectTypeOptions{}
	po.Name = "Post"
	po.New = func(db *DB) interface{} {
		return User{DB: db}
	}
	po.Get = func(m map[KeyValueKey][]byte, db *DB) (interface{}, []error) {
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
		p := Post{DB: db}

		if id, ok := m[KeyValueKey{Main: "ID"}]; ok {
			f, err := db.FT["uint64"].Get(id)
			if err != nil {
				e = append(e, err)
				return nil, e
			}
			p.ID = f.(uint64)
		} else {
			e = append(e, errors.New("The Data from the DB has no ID"))
			return nil, e
		}
		if f, ok := m[KeyValueKey{Main: "body"}]; ok {
			v, err := db.FT["string"].Get(f)
			if err != nil {
				e = append(e, err)
			} else {
				p.body = v.(string)
			}
		}

		return p, e
	}
	po.Set = func(i interface{}, db *DB) (u map[KeyValueKey][]byte, e []error) {
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
		d := i.(Post)
		db.RLock()
		defer db.RUnlock()
		if d.ID > 0 {
			u[KeyValueKey{Main: "ID"}], _ = db.FT["uint64"].Set(d.ID)
		}
		if d.body != "" {
			u[KeyValueKey{Main: "body"}], _ = db.FT["string"].Set(d.body)
		}
		return
	}
	po.Validate = func(i interface{}, db *DB) (interface{}, []error) {
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
		d := i.(Post)
		x, y, z := db.OT["Post"].Fields["body"].Validate(d.body, db)
		if !x {
			e = append(e, z)
		} else {
			d.body = y.(string)
		}
		return d, e
	}
	fv := FieldValidation{}
	po.Fields = make(map[string]FieldOptions)
	po.Fields["body"] = FieldOptions{Indexed: false, FieldType: "string", Validate: fv.String("body", 20, -1, false, false, false)}
	return po
}

func GetFriendLinkOption() LinkTypeOptions {
	fl := LinkTypeOptions{}
	fl.Type = 1
	fl.Name = "Friend"
	fl.From = "User"
	fl.To = "User"
	fl.New = func(db *DB) interface{} {
		return Friend{DB: db, created: time.Now()}
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
		friend := Friend{DB: db}
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
				friend.created = v.(time.Time)
			}
		}
		return friend, e
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
		u[KeyValueKey{Main: "created"}], _ = db.FT["date"].Set(d.created)
		return
	}
	fl.Validate = func(i interface{}, db *DB) (interface{}, []error) {
		e := make([]error, 0)
		j, ok := i.(Friend)
		if !ok {
			e = append(e, errors.New("Porvided interface is not of the type Friend"))
			return i, e
		}
		//log.Print("**  " + strconv.Itoa(int(j.FROM)) + " ****** " + strconv.Itoa(int(j.TO)))
		if j.FROM == j.TO {
			log.Print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
			e = append(e, errors.New("Friend link cant have the same FROM and TO"))
			return i, e
		}
		return i, e
	}
	//fv := FieldValidation{}
	fl.Fields = make(map[string]FieldOptions)
	fl.Fields["accepted"] = FieldOptions{FieldType: "bool"}
	fl.Fields["created"] = FieldOptions{FieldType: "date"}

	return fl
}

func GetAuthorLinkOption() LinkTypeOptions {
	fl := LinkTypeOptions{}
	fl.Type = 1
	fl.Name = "Author"
	fl.From = "User"
	fl.To = "Post"
	fl.New = func(db *DB) interface{} {
		return Author{DB: db}
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
		author := Author{DB: db}
		if f, ok := m[KeyValueKey{Main: "FROM"}]; ok {
			v, err := db.FT["uint64"].Get(f)
			if err != nil {
				e = append(e, err)
			} else {
				author.FROM = v.(uint64)
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
				author.TO = v.(uint64)
			}
		} else {
			e = append(e, errors.New("The Data from the DB has no TO field set"))
			return nil, e
		}
		return author, e
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
		d, ok := i.(Author)
		if !ok {
			e = append(e, errors.New("The Provided struct is not of type Author"))
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
		return
	}
	fl.Validate = func(i interface{}, db *DB) (interface{}, []error) {
		e := make([]error, 0)
		j, ok := i.(Author)
		if !ok {
			e = append(e, errors.New("Porvided interface is not of the type Author"))
			return i, e
		}
		if j.FROM == j.TO {
			e = append(e, errors.New("Author link cant have the same FROM and TO"))
			return i, e
		}
		return i, e
	}
	fl.Fields = make(map[string]FieldOptions)
	return fl
}

func GetLikeLinkOption() LinkTypeOptions {
	fl := LinkTypeOptions{}
	fl.Type = 1
	fl.Name = "Like"
	fl.From = "User"
	fl.To = "Post"
	fl.New = func(db *DB) interface{} {
		return Like{DB: db}
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
		like := Like{DB: db}
		if f, ok := m[KeyValueKey{Main: "FROM"}]; ok {
			v, err := db.FT["uint64"].Get(f)
			if err != nil {
				e = append(e, err)
			} else {
				like.FROM = v.(uint64)
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
				like.TO = v.(uint64)
			}
		} else {
			e = append(e, errors.New("The Data from the DB has no TO field set"))
			return nil, e
		}
		return like, e
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
		d, ok := i.(Like)
		if !ok {
			e = append(e, errors.New("The Provided struct is not of type Like"))
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
		return
	}
	fl.Validate = func(i interface{}, db *DB) (interface{}, []error) {
		e := make([]error, 0)
		return i, e
	}
	fl.Fields = make(map[string]FieldOptions)
	return fl
}

func InitDB(opt Options) *DB {
	db := &DB{}
	db.Init(opt)
	db.AddObjectType(GetUserOption())
	db.AddObjectType(GetPostOption())
	db.AddLinkType(GetFriendLinkOption())
	db.AddLinkType(GetAuthorLinkOption())
	db.AddLinkType(GetLikeLinkOption())
	return db
}