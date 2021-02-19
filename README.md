# Simpleg

Simple, lightweight, Activerecord like, embeded graph database written in go.

# Why Simpleg?
Graph databases are cool (check [here](https://dzone.com/articles/what-are-the-pros-and-cons-of-using-a-graph-databa) to know more) but they are quite expensive to use, Google and Facebook can spin up hundreds of 64GB memory servers to handle their graph database but i doubt if there is anyone out there willing to setup an 8GB memory server to host the database for a pet project. This is where Simpleg comes in removing the overkill features like accepting data over a network, dedicated interaction language and multiple server coordination features to give you a database you can host in your 1GB VPS without any issues while providing all the necessary graph database features. Simpleg uses [Badger](https://github.com/dgraph-io/badger/) as it's internal key value store thereby bringing the power of one of the top key value databases (also used by [Dgraph](https://dgraph.io/) internally) out there. Simpleg is also built with a flexible and easily extensible core giving you the opprtunity to extend with functionalities that suite your needs without modifications to the core!



# Installation
Add Simpleg to your `go.mod` file:
```
module github.com/x/y

go 1.14

require (
        github.com/timmytune/simpleg/simpleg latest
)
```



# Initialization & Configuration

Simpleg is structured like an ORM and like many ORMs you are going to be writing a substantial amount configuration.

## Initializating the DB

```go
import (
	simpleg "github.com/timmytune/simpleg/simpleg"
)

func main() {
	//get the default configuration for simpleg. This configuration is of type simpleg.Options
	dbOptions := simpleg.DefaultOptions()
	//get a pointer to a new instance of simpleg.DB
	db := simpleg.GetNewDB()
	//Initialize DB with options
	db.Init(opt)
	...
```
## ObjectType 
An Object in simpleg is the same thing as vertex in other graph databases. An objectType is an abstract representation of a set of objects i.e models in Laravel and Mongoose. ObjectType In simpleg needs to be initialized in order to use. To initialize an objectType you have to provide an instance of simpleg.ObjectTypeOptions. Check below for implementation.

```go

import (
	simpleg "github.com/timmytune/simpleg/simpleg"
)
//type of object simpleg will be using. Everytime simpleg wants to return data of the objectType from the db it will come as an instance of this struct 
type Object struct {
	//Mandatory field, instance of the current DB
	DB       *DB
	//Mandatory field, identifier of the current object
	ID       uint64
	//Optional field that is a part of this object
	field1   string
	//Optional field that is a part of this object
	field2   bool
}

//Function to get Options for the objext we are addding to the DB
func GetObjectOptions() simpleg.ObjectTypeOptions {
	//Initialize a new instance of simpleg.ObjectTypeOptions
	ot := simpleg.ObjectTypeOptions{}
	//Set the name the object will use.
	ot.Name = "Object"
	//Called everytime there is a request for a new object of this type. 
	ot.New = func(db *simpleg.DB) interface{} {
		return Object{DB: db, field1: "Default value"}
	}
	//Called everytime simpleg is converting key value returned from db to object of this type
	ot.Get = func(m map[KeyValueKey][]byte, db *simpleg.DB) (interface{}, []error) {
	e := make([]error, 0)
	db.RLock()
	defer db.RUnlock()
	u := Object{DB: db}
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
	if f, ok := m[KeyValueKey{Main: "field1"}]; ok {
		v, err := db.FT["string"].Get(f)
		if err != nil {
			e = append(e, err)
		} else {
			u.field1 = v.(string)
		}
	}
	if f, ok := m[KeyValueKey{Main: "field2"}]; ok {
		v, err := db.FT["bool"].Get(f)
		if err != nil {
			e = append(e, err)
		} else {
			u.field2 = v.(bool)
		}
	}
	return u, e
	}
	//Called everytime simpleg wants to convert object of this type to key/value pair to save to DB
	ot.Set = func(i interface{}, db *simpleg.DB) (u map[KeyValueKey][]byte, e []error) {
	u = make(map[KeyValueKey][]byte)
	e = make([]error, 0)
	d := i.(Object)
	db.RLock()
	defer db.RUnlock()
	if d.ID > uint64(0) {
		u[KeyValueKey{Main: "ID"}], _ = db.FT["uint64"].Set(d.ID)
	}
	if d.field1 != "" {
		u[KeyValueKey{Main: "field1"}], _ = db.FT["string"].Set(d.firstName)
	}
	if d.field2 != "" {
		u[KeyValueKey{Main: "field2"}], _ = db.FT["bool"].Set(d.firstName)
	}
	return
	}
	//Provide validation 
	fv :=  simpleg.FieldValidation{}
	//Initialize fields
	ot.Fields = make(map[string]FieldOptions)
	//Add the configuration for each individual field
	ot.Fields["field1"] = FieldOptions{
							//set wether this field would be indexed or not
							Indexed: true,
							//Set if this field is an advanced one one or not
							Advanced: false, 
							//The fieldtype this field is attached to
							FieldType: "string", 
							//Validation function to use when saving this field to DB
							Validate: fv.String("field1", 3, 20, true, true, false)}
	ot.Fields["field2"] = FieldOptions{
							Indexed: true, 
							Advanced: false, 
							FieldType: "bool"}
	//Called everytime an object of this type is about to be saved into the database. it checks if the object is valid and transform its fields if you want them transformed
	ot.Validate = func(i interface{}, db *simpleg.DB) (interface{}, []error) {
	e := make([]error, 0)
	db.RLock()
	defer db.RUnlock()
	d := i.(Object)
	x, y, z := db.OT["Object"].Fields["field1"].Validate(d.field1, db)
	if !x {
		e = append(e, z)
	} else {
		d.field1 = y.(string)
	}
	return d, e
	}

	return ot
	}

	//Register this object type with the DB
	db.AddObjectType(GetObjectOptions())
```

You must be wondering this a lot of code and you are right but majority of the code checks for errors. Also Simpleg is structured this way to provide a lot of features without sacrificing performance. Here are some of the things you can achieve  
-   Setting default values 
-   Virtual fields
-   Validation 
-   Database field names can be differnt from object field names
  And many more.   

## LinkTypes
Links in simpleg are the same thing as edges in in other graph databases. It signifies a relationship between two Objects. A linkType is an abstract form of a link. All links are a type of Linktype and all links can only be of one linkType. There are three types of links in simpleg.
-   One to one:- This is the type of link that links two objects of the same type and relationship stil means the same thing if reversed. An example is facebook friends, it has the same meaning from the perspective of both users that are friends. There is only one version of information is stored in the database.
-   One to another:- This is the type of link that links two objects of separate object types where the relationship cannot be reversed. An example is the relationship between a facbook user and his/her post, a user can make a post but a post cannot make a user.There is only one version of information stored in the database.
-   One to One with opposite not meaning same thing:- This is the type of link that links two objects of the same type but with the possibility of another relationship between same objects. An example is that of a landlord and a tenant, it is a one way relationship but it is possible for the tenant to own a beach house where landlord can then be the tenant of his own tenant. another example is twitter following, i can follow you but it is possible that you are not following me. This kind of relationship stores two different versions of the relationship in the database.
  
When it comes to representation and storage in the DB Links and Objects are stored in the same way and both of them can have serveral fields. There are differences though, objects must have fields while fields are not mandatory for links, links too don't have a unique identifier, they rely on the objects they are connecting. below is the configuration required to add a linkType to simpleg, Fields have been ommited but take note they can be added if you want them.

```go
//link object 
type Link struct {
	//instance of DB (mandatory)
	DB   *DB
	//FROM id of the object this link is originating from (Mandatory)
	FROM uint64
	//TO id of the Object this link is linking to (Mandatory)
	TO   uint64
}

func GetLinkOptions() simpleg.LinkTypeOptions {
	//Initialize the simpleg link option type
	fl := simpleg.LinkTypeOptions{}
	//Here we specify the link type, 
	//1 stands for links where both sides of the link are the same and the realationship can only be established once
	//2 stands for links where both sides are of different types 
	//3 stands for links where both sides are of the same type but multiple relationships can be established
	fl.Type = 3
	//Name of link as stored and retrieved from the DB
	fl.Name = "Link"
	//Type of object this link will be originating from
	fl.From = "Object"
	//Type of object this link will be leading to
	fl.To = "Object"
	//Return a new instance of the link, called internally
	fl.New = func(db *DB) interface{} {
		return Link{DB: db}
	}
	//Called everytime simpleg wants to convert data gotten from the DB into an instance of the Link
	fl.Get = func(m map[KeyValueKey][]byte, db *DB) (interface{}, []error) {
		e := make([]error, 0)
		db.RLock()
		defer db.RUnlock()
		like := Link{DB: db}
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
	//Called everytime simpleg wants to convert struct of this linktype to data to be saved in the DB
	fl.Set = func(i interface{}, db *DB) (u map[KeyValueKey][]byte, e []error) {
		u = make(map[KeyValueKey][]byte)
		e = make([]error, 0)
		d, ok := i.(Link)
		if !ok {
			e = append(e, errors.New("The Provided struct is not of type Link"))
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
	//register linkType with DB
	db.AddLinkType(GetLinkOptions())

```
Now that we can add linkTypes to the DB, it is time to move on to fieldTypes

## FieldTypes
FieldTypes in simpleg are abstract forms of individual properties of Objects and Links. They are responsible for 
-   Initializing values of the fields
-   Converting values from their binary form to their actual form and vice varsa
-   Comparing values of the field type
-   Getting the comparing instruction for indexed fields
Simpleg comes with 5 filedtypes and you can create as many as you want, to create your own fieledtype you have to implement the interface simpleg.FieldType, below is an example of uint64 that comes with simpleg
```go
import (
	"encoding/binary"
	"errors"
)

type FieldTypeUint64 struct {
}
//Get options for this fieldtype
func (f *FieldTypeUint64) GetOption() map[string]string {
	m := make(map[string]string)
	//fieldType name
	m["Name"] = "uint64"
	//wether to allow indexing of fields implementing this fieldType
	m["AllowIndexing"] = "1"
	return m
}
//Return a new Zero version of this fieldType
func (f *FieldTypeUint64) New() interface{} {
	var r uint64 = 0
	return r
}
//Convert values of this filedType to an []byte for saving to DB
func (f *FieldTypeUint64) Set(v interface{}) ([]byte, error) {
	d, ok := v.(uint64)
	if !ok {
		return nil, errors.New("Interface is not of type of uint64")
	}

	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, d)
	return buf, nil
}
//Convert []byte to value of this fieldtype
func (f *FieldTypeUint64) Get(v []byte) (interface{}, error) {
	i, err := binary.Uvarint(v)
	return i, err
}
//Compare between two []byte of this fieldType
func (f *FieldTypeUint64) Compare(typ string, a []byte, b []byte) (bool, error) {
	var err error
	ia, _ := binary.Uvarint(a)
	ib, _ := binary.Uvarint(b)

	switch typ {
	case "==":
		return (ia == ib), err
	case "!=":
		return (ia != ib), err
	case ">":
		return (ia > ib), err
	case ">=":
		return (ia >= ib), err
	case "<":
		return (ia < ib), err
	case "<=":
		return (ia <= ib), err
	default:
		return false, errors.New("fieldtype uint64 does not support this comparison operator")
	}

}
//Return strings for index comparison 
func (f *FieldTypeUint64) CompareIndexed(typ string, a interface{}) (string, string, error) {
	var err error
	g, err := f.Set(a)
	if err != nil {
		return "", "", err
	}
	s := string(g)
	switch typ {
	case "==":
		return s, "==", err
	case ">":
		return s, ">", err
	case ">=":
		return s, ">=", err
	case "<":
		return s, "<", err
	default:
		return "", "", errors.New("fieldtype uint64 does not support this comparison operator for indexed field")
	}

}
//Register Fieldtype with DB
db.AddFieldType(simpleg.FieldTypeOptions{Name: "uint64", AllowIndexing: true}, &FieldTypeUint64{})
```
Simpleg completely relies on fieldTypes to handle and compare fields and for this reason the fieldtypes are responsible for the types of comparison they allow. The fieldTypes are also responsible for for providing instruction on how indexing is applied, will talk more about that later. Listed below are the comparison operators of the inbuilt fieldtypes in simpleg.



### FieldType bool: works with the golang bool variable type
| Compare         | Indexed      | Description  |
| ------------- |:-------------:| -----:|
|  ==           |               | Checks if two varaibles are equal |
|  !=           |               |   Checks if two varaibles are not equal |



### FieldType date: works with the golang time.Time type
| Compare         | Indexed      | Description  |
| ------------- |:-------------:| -----:|
|  ==           |       ==        | Checks if two varaibles are equal |
|  !=           |               |   Checks if two varaibles are equal |
|  >           |        >       |   Checks if one variable is greater than another |
|  >=           |       >=       |   Checks if one variable is greater than or equal to another |
|  <           |        <     |   Checks if one variable is less than another |
|  <=           |       <=        |   Checks if one variable is less than or equal to another |


### FieldType int64: works with the golang int64 type
| Compare         | Indexed      | Description  |
| ------------- |:-------------:| -----:|
|  ==           |       ==        | Checks if two varaibles are equal |
|  !=           |               |   Checks if two varaibles are equal |
|  >           |        >       |   Checks if one variable is greater than another |
|  >=           |       >=       |   Checks if one variable is greater than or equal to another |
|  <           |        <     |   Checks if one variable is less than another |
|  <=           |       <=        |   Checks if one variable is less than or equal to another |

### FieldType string: works with the golang string type
| Compare         | Indexed      | Description  |
| ------------- |:-------------:| -----:|
|  ==           |       	    |   Checks if two varaibles are equal |
|  !=           |               |   Checks if two varaibles are equal |
|  contains     |               |   Checks if one string variable is contained in another |
|  NoCaseEqual  |               |   Checks if two strings are equal with case insesitivity |
|  HasSuffix    |               |   Checks if one string is a suffix of another |
|  prefix       |    prefix     |   Checks if one string is a prefix of another |

### FieldType uint64: works with the golang uint64 type
| Compare         | Indexed      | Description  |
| ------------- |:-------------:| -----:|
|  ==           |       ==      | Checks if two varaibles are equal |
|  !=           |               |   Checks if two varaibles are equal |
|  >            |        >      |   Checks if one variable is greater than another |
|  >=           |       >=      |   Checks if one variable is greater than or equal to another |
|  <            |        <      |   Checks if one variable is less than another |
|  <=           |       <=      |   Checks if one variable is less than or equal to another |

We have talked about configuration of fieldTypes, ObjectTypes and LinkTypes, we still have to talk about advanced fieldtypes but with what we  have right now we can use simpleg so we will talk about usage instead.

# Using Simpleg
So to complete initialization of simpleg 

```go
...
	//Register linkType with DB
	db.AddLinkType(GetLinkOptions())
	//Register objectType with DB
	db.AddObjectType(GetObjectOptions())
	//Start DB
	db.Start()
}
```

## Storing data in simpleg
To save objects to the DB we need to get an instance of the object and add data to it
```go
	//Gets a new instance of objecttype Object, returned result is of type simpleg.GetterRet
	ret := db.Get("object.new", "Object")
	//errors not checked for simplicity
	o, _ := ret.Data.(Object)
	o.field1 = "Awesome string"
	o.field2 = true
```
This saves an instance of an object into the database. 
First parameter is the instruction in this case it is  "save.object"
Second parameter is the name of the objectType about to be saved
Third parameter is the instance of an Object to be saved
```go
	ret2 := db.Set("save.object", "Object", o)
```
The function returns an instance of simpleg.SetterRet with the ID of the object saved and an arrays of errors if any. If the ID of the field is not set the DB creates a new instance and assigns the instance a new ID, this ID is then returned. IF the ID is provided the instance with the specified ID is updated in the DB. This is enough to save objects but most times we might just want to update a single field and using this will waste resources. To update an objects field we use the same function with a different instruction.
```go
	ret3 := db.Set("save.object.field", "Object", ret2.ID, "field1", "Updated string")
```
To save links into the DB we use the same function with a different instruction.
The second parameter is the LinkType name
The third parameter is an instance of the linkType about to be saved.
```go
	ret := db.Get("link.new", "Link")
	//errors not checked for simplicity
	l, _ := ret.Data.(Link)
	l.FROM = uint64(1)
	l.TO = uint64(2)
	ret3 := db.Set("save.link", "Link", ret2.ID, l)
```
Take note, you have to provide a link with the 'TO' and 'FROM' set to ID of valid objects or else you get an error. If the link already exists it will be updated but if not it will be created. Also if the linktype is of the type 1 and there exists an opposite link, the opposite link will be updated instead. You can also save a link's field with the same function. 
The third parameter is the 'FROM' field of the link
The fourth parameter is the 'TO' field of the link
The fifth field is the name of the field to be saved or updated 
The sixth field is the value the field is updated with
```go
	ret3 := db.Set("save.link.field", "Link", uint64(1), uint64(2) "linkField", "Updated field value")
```
db.Set is goroutine safe and can be called from different goroutines at the same time. The function blocks until a result is returned.
## Getting sigle single object/link in simpleg
To get data from simpleg you use the db.Get function. Like db.Set db.Get is goroutine safe and blocks until result is returned. You have already seen it's usage for geting a new link or object so we move to using it to retieve a single instance of an object.
The first parameter is the instruction
The second parameter is the object type name
The third is the uique ID of the object to be returned  
```go
	ret := db.Get("object.single", "Object", uint64(2))
	obj, _ := ret.data.(Object)
```
Getting a single instance of a link is done with the same function.
Second parameter is the Link Type name 
Third parameter is the direction of the link. Link direction indicates wether the link requested is from field 'FROM' to field 'TO' with '->' or from Field 'TO' to field 'FROM' with '<-', there is also a third direction '-' that indicates either of the previous links but the third is not supported by this function
Fouth parameter is the ID of the object the link is linking from
Fifth parameter is the ID of the object the link is linking to
```go
ret := db.Get("link.single", "Link", "->", uint64(1), uint64(2))
	lnk, _ := ret.data.(Link)
```
## Getting multiple object/link from simpleg
To get multiple objects/links in simpleg you need a nodequery and a query
### Nodequery
A nodequery is used to create instructions for retrieving an array of objects/links that meet a certain critaria based on field comparison. To initialize a nodequery
```go
	n := simpleg.NodeQuery{}
	//name given to this nodequery so we can identify it when returning it in a Query
	n.Name("da")
	//Indicate this nodequery will be used to retieve objects, the parameter in the objectType name
	n.Object("User")
	//Used to give comparison instruction, 
	//The first parameter is the field in the object to compare against
	//The second parameter is the type of comparison to be done
	//The third parameter is the value the comparison will be done against. for instance the below function checks if 'some prefix' is a 'prefix' of values in 'field1'
	n.Q("field1", "prefix", "some prefix")
	//Ordering of the returned result,
	//First parameter is the field to order by
	//Second parameter determines how the ordering is done either assending 'asc' or decending 'dsc'
	n.Order("field1", "dsc")
	//The number of results to return
	n.Limit(100)
	//The number of results to skip
	n.Skip(10)
	n.Q("field2", "==", true)
```

The first n.Q function call of a nodequery determines wether the query will use indexing or not, If the field provided is indexed indexing is used, if there are other indexed field referenced in the query they are ignored. Simpleg works this way for simplicity sake and also to minimize memory usage. Nodequery functions are also chainable so this is valid
```go
	n.Name("da").Object("Object").Q("field1", "prefix", "yes").Order("ID", "dsc").Limit(100)
```
 To execute the instructions of a nodequery we are going to be discussing another key feature of Simpleg, the Query
### Query
Query in Simpleg is responsible for retieving data and 'db.Get' uses a Query internally for data retrieval. Query retrievies data by executing instructions it is provided. To understand how a Query works we need to walk through a complete example.
```go
	//Initialize a Query
	q := db.Query()
	//Get a new nodequery
	n := simpleg.NodeQuery{}
	//initialize Nodequery
	n.Name("da").Object("Object").Q("field1", "prefix", "yes").Order("ID", "dsc").Limit(100)
	//To add an instruction to a Query 
	//The first parameter is the instruction name 
	//The second parameter is the instruction parameter
	//The function can take more than one pararameter infact the type is of ...interface{} so you can provide unending amount of parameters.
	//This calls the internal function called "object" which loads Objects from the database based on the instructions in the node query and stores the result in a variable called "da" (as specified in nodequery).  
	q.Do("object", n)
	//This function returns the result of the Query.
	//The first parameter specifies the return type which can only be 'single', 'array', 'map' or 'skip' 
	//if you use 'single' you will need to provide a third parameter which is the index of an array. 
	//if you use 'map' you can provide multiple variable names and they will all be returned as a map of type map[string]interface{}
	//if you use 'array' the expected result will be of type []interface
	//if you use 'skip' then the return funtion is skipped and you have to provide another function that will work in its place, we will talk more about functions later 
	retGet := q.Return("array", "da")
```
The returned result is of the type simpleg.GetterRet 

