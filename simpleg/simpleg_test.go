package simpleg

import (
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

func TestMain(m *testing.M) {

	s = FieldTypeBool{}
	z = FieldTypeString{}
	i = FieldTypeInt64{}
	u = FieldTypeUint64{}
	d = FieldTypeDate{}

	opt := DefaultOptions()
	db = InitDB(opt)
	db.Start()

	ret := m.Run()

	db.Close()
	os.RemoveAll(opt.DataDirectory)

	os.Exit(ret)

}

func TestAll(t *testing.T) {
	//------------------------ set of 10 friend node objects

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 9; i++ {
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
				t.Error("simpleg.Set Failed Test got:", re)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	Log.Printf("++++ Test Set 10 friend Object took %s", elapsed)

	var w1 sync.WaitGroup
	start = time.Now()
	for i := 0; i < 10; i++ {
		w1.Add(1)
		go func() {
			defer w1.Done()
			ret := db.Get("object.new", "Post")
			if len(ret.Errors) > 0 {
				t.Error("simpleg.Set Failed Test get user:", ret)
			}
			u, _ := ret.Data.(Post)
			u.body = "Yinka sdgsgsdgsdfsdfsfdd  sd gsdgsgs dgsdg s ds gsd gsdgg sd s sdgsd gsdfsdff s sdfsdfs dfsdfsddf  sdfsdfsf asfasf a fasff fsfas asfsf"
			re := db.Set("save.object", "Post", u)
			if len(re.Errors) != 0 {
				t.Error("simpleg.Set Failed Test got:", re)
			}
		}()
	}
	w1.Wait()
	elapsed = time.Since(start)
	Log.Printf("++++ Test Set 10 friend Object took %s", elapsed)

	//--------------------------------- get one friend object
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
	//--------------------------------- Update object field
	ret = db.Set("save.object.field", "User", uint64(2), "lastName", "Adebara")
	if len(ret.Errors) != 0 {
		t.Error("simpleg.Update lastName Failed Test got:", ret)
	}

	ret = db.Set("save.object.field", "User", uint64(2), "email", "yinka@gmail.com")
	if len(ret.Errors) != 0 {
		t.Error("simpleg.Update user email Failed Test got:", ret)
	}

	ret = db.Set("save.object.field", "User", uint64(2), "age", int64(121))
	if len(ret.Errors) == 0 {
		t.Error("simpleg.Update Failed Test got:", ret)
	}
	//Get preexisting objects
	//rand.Seed(time.Now().UnixNano())

	var w sync.WaitGroup
	start = time.Now()
	for i := 1; i < 10; i++ {
		w.Add(1)
		go func() {
			defer w.Done()
			retGet := db.Get("object.single", "User", uint64(8))
			if len(retGet.Errors) > 0 {
				t.Error("simpleg.Set Failed Test get user:", retGet)
			}
			_, ok := retGet.Data.(User)
			if !ok {
				t.Error("simpleg.Set Failed Test get user: convert ret to User", retGet)
			}

		}()
	}
	w.Wait()
	elapsed = time.Since(start)
	Log.Printf("++++ Test Get Object took %s", elapsed)

	time.Sleep(10 * time.Second)
	//-----------------------------------------Get objects with raw query
	start = time.Now()
	q := db.Query()
	n := NodeQuery{}
	n.Name("da").Object("User").Q("age", ">=", int64(7)).Order("ID", "dsc").Limit(100)
	q.Do("object", n)
	retGet := q.Return("array", "da")
	if len(retGet.Errors) > 0 {
		t.Error("simpleg.Query Failed Test get users:", retGet.Errors)
	} else {
		log.Print("---- 4444 ---- ", len(retGet.Data.([]interface{})))
	}
	q2 := db.Query()
	n2 := NodeQuery{}
	n2.Name("da").Object("Post").Order("ID", "dsc").Limit(100)
	q2.Do("object", n2)
	retGet7 := q2.Return("array", "da")
	if len(retGet7.Errors) > 0 {
		t.Error("simpleg.Query Failed Test get users:", retGet7.Errors)
	} else {
		log.Print("---- 5555 ---- ", len(retGet7.Data.([]interface{})))
	}
	//Log.Print("-------------------------------------------- Test Get Objects ", len(retGet7.Data.([]interface{})))
	//wg.Wait()
	elapsed = time.Since(start)
	Log.Printf("++++ Test Get Objects with raw query took %s", elapsed)

	//--------------------------------------- Set Friend Links up to 800
	//var wg2 sync.WaitGroup

	start = time.Now()
	for i := 0; i < 10; i++ {
		//wg2.Add(1)
		//func() {
		//defer wg2.Done()
		ret := db.Get("link.new", "Friend")
		if len(ret.Errors) > 0 {
			t.Error("simpleg.Set Failed Test get Friend:", ret)
		}
		f, ok := ret.Data.(Friend)
		if !ok {
			t.Error("simpleg.SetLink Failed Test get user:", ret)
		}
		f.FROM = uint64(i + 1)
		f.TO = uint64(9 - i + 1)
		f.accepted = false
		if (i + 1) != (9 - i + 1) {
			r := db.Set("save.link", "Friend", f)
			if len(r.Errors) > 0 {
				t.Error("simpleg.Set Failed Test set Friend:", r.Errors)
			}
		}
		time.Sleep(1 * time.Second)
		//	}()
	}

	elapsed = time.Since(start)
	Log.Printf("++++ Test Set Links %s", elapsed)

	r := db.Set("save.link.field", "Friend", uint64(1), uint64(8), "accepted", true)
	if len(r.Errors) > 0 {
		t.Error("simpleg.Set Failed Test set Friend:", r)
	}
	//-------------------------------------------------------- Test Get Link
	ret2 := db.Get("link.single", "Friend", "->", uint64(1), uint64(7))
	if len(ret2.Errors) > 0 {
		t.Error("simpleg.Get Failed Test get Friend:", ret2)
	}

	n5 := NodeQuery{}
	q5 := db.Query()
	n5.Name("da").Link("Friend", "->").Limit(10).Q("accepted", "==", true)
	q5.Do("link", n5)
	ret3 := q5.Return("array", "da")
	if len(ret3.Errors) > 0 {
		Log.Printf("---- Test Get Link had the following errors %s", ret3.Errors)
		t.Error("simpleg.Get Failed Test get Friend:", ret3)
	}
	Log.Print("---- cccccccccccc Test Get Link returned ", len(ret3.Data.([]interface{})))
	n4 := NodeQuery{}
	q4 := db.Query()
	n4.Name("da").Link("Friend", "->").Limit(100)
	q4.Do("link", n4)
	ret4 := q4.Return("array", "da")
	if len(ret4.Errors) > 0 {
		Log.Printf("---- Test Get Link had the following errors %s", ret4.Errors)
		t.Error("simpleg.Get Failed Test get Friend:", ret4)
	}
	Log.Print("---- ddddddddddddd Test Get Link returned ", len(ret4.Data.([]interface{})))

	start = time.Now()
	for i := 0; i < 10; i++ {
		//wg2.Add(1)
		//func() {
		//defer wg2.Done()
		reta := db.Get("link.new", "Like")
		if len(reta.Errors) > 0 {
			t.Error("simpleg.Set Failed Test get Like:", reta)
		}
		f, ok := reta.Data.(Like)
		if !ok {
			t.Error("simpleg.SetLink Failed Test get user:", reta)
		}
		f.FROM = uint64(i + 1)
		f.TO = uint64(9 - i + 1)
		r := db.Set("save.link", "Like", f)
		if len(r.Errors) > 0 {
			t.Error("simpleg.Set Failed Test set Like:", r)
		}
		//	}()

	}

	elapsed = time.Since(start)
	Log.Printf("++++ Test Set Links %s", elapsed)
	time.Sleep(1 * time.Second)

	for i := 0; i < 10; i++ {
		ret := db.Get("link.new", "Friend")
		if len(ret.Errors) > 0 {
			t.Error("simpleg.Set Failed Test get Friend:", ret)
		}
		f, ok := ret.Data.(Friend)
		if !ok {
			t.Error("simpleg.SetLink Failed Test get user:", ret)
		}
		f.FROM = uint64(2)
		f.TO = uint64(9 - i + 1)
		f.accepted = false
		if 2 != (9-i+1) && 0 != (9-i+1) {
			r := db.Set("save.link", "Friend", f)
			if len(r.Errors) > 0 {
				t.Error("simpleg.Set Failed Test set Friend:", r, f)
			}
		}

		ret2 := db.Get("link.new", "Friend")
		if len(ret2.Errors) > 0 {
			t.Error("simpleg.Set Failed Test get Friend:", ret)
		}
		f2, ok := ret2.Data.(Friend)
		if !ok {
			t.Error("simpleg.SetLink Failed Test get user:", ret)
		}
		f2.TO = uint64(10)
		f2.FROM = uint64(9 - i + 1)
		f2.accepted = false
		if 10 != (9-i+1) && 0 != (9-i+1) {
			r := db.Set("save.link", "Friend", f2)
			if len(r.Errors) > 0 {
				t.Error("simpleg.Set Failed Test set Friend:", r)
			}
		}
		time.Sleep(1 * time.Second)
	}

	time.Sleep(10 * time.Second)

	n6 := NodeQuery{}
	q6 := db.Query()
	n6.Name("da").Link("Friend", "->").Limit(100).Q("FROM", "==", uint64(2))
	q6.Do("link", n6)
	ret6 := q6.Return("array", "da")
	if len(ret6.Errors) > 0 {
		t.Error("simpleg.Get Failed Test get Friend:", ret6.Errors)
	} else {
		log.Print("---- 5.5.5.5 ---- ", len(ret6.Data.([]interface{})))
	}

	n7 := NodeQuery{}
	q7 := db.Query()
	n7.Name("da").Link("Friend", "-").Limit(100).Q("FROM", "==", uint64(10))
	q7.Do("link", n6)
	ret7 := q6.Return("array", "da")
	if len(ret7.Errors) > 0 {
		t.Error("simpleg.Get Failed Test get Friend:", ret7.Errors)
	} else {
		log.Print("---- 6666 ---- ", len(ret7.Data.([]interface{})))
	}

	startUser := NodeQuery{}
	startUser.Object("User").Q("ID", "==", uint64(2))

	friends := NodeQuery{}
	friends.Link("Friend", "-")

	mutual := NodeQuery{}
	mutual.Object("User").Name("mutual")

	friends2 := NodeQuery{}
	friends2.Link("Friend", "-")

	endUser := NodeQuery{}
	endUser.Object("User").Q("ID", "==", uint64(10)).Limit(100)

	query := db.Query()
	query.Do("graph.p", startUser, friends, mutual, friends2, endUser)
	retQuery := query.Return("map", "mutual")
	if len(retQuery.Errors) > 0 {
		Log.Printf("---- Test Get Link had the following errors %s", retQuery.Errors)
		t.Error("simpleg.Get Failed Test get Friend:", retQuery)
	} else {
		log.Print("---- 8888 ---- ", len(retQuery.Data.(map[string]interface{})["mutual"].([]interface{})))
	}
}
