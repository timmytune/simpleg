/*
 * Copyright 2021 Adedoyin Yinka and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
var f FieldTypeFloat64
var db *DB

func TestMain(m *testing.M) {

	s = FieldTypeBool{}
	z = FieldTypeString{}
	i = FieldTypeInt64{}
	u = FieldTypeUint64{}
	d = FieldTypeDate{}
	f = FieldTypeFloat64{}

	db = InitDB()
	db.Start()

	ret := m.Run()

	db.Close()
	//os.RemoveAll("/data/simpleg")
	os.Exit(ret)

}

func TestAll(t *testing.T) {
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 9; i++ {
		time.Sleep(500 * time.Millisecond)
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
			u.date = time.Now()
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

	j := db.Get("object.new", "User")
	if len(j.Errors) > 0 {
		t.Error("simpleg.Set Failed Test get user:", j)
	}
	u9, _ := j.Data.(User)
	u9.firstName = "Yinka"
	u9.lastName = "Adedoyin"
	u9.email = "timmytune002@gmail.com"
	u9.age = int64(19)
	u9.ID = uint64(4)
	v := db.Set("save.object", "User", u9)
	if len(v.Errors) != 0 {
		t.Error("simpleg.Set Failed Test got:", v)
	}

	time.Sleep(2 * time.Second)

	var w1 sync.WaitGroup
	start = time.Now()
	for i := 0; i < 20; i++ {
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

	u.ID = ret.ID
	log.Print("vvvvv 1 :-", ret)
	xx, xe := u.addActivity(time.Now(), "windows-10", uint64(1))
	xx, xe = u.addActivity(time.Now(), "xp-360", uint64(2))
	xx, xe = u.addActivity(time.Now(), "android", uint64(3))
	xx, xe = u.addActivity(time.Now(), "ios", uint64(4))
	xx, xe = u.addActivity(time.Now(), "symbian", uint64(5))
	xx, xe = u.addActivity(time.Now(), "windows-phone", uint64(100))
	log.Print("vvvvv 2 :-", xx, xe)
	if len(xe) > 0 {
		t.Error("simpleg.Set Failed Add user Activity: ", xx, xe)
	}
	xe = u.activities.Save()
	if len(xe) > 0 {
		t.Error("simpleg.Set Failed Save user Activity: ", xe)
	}

	time.Sleep(2 * time.Second)

	log.Print("vvvvv 3 :-", u.activities)
	_, xx = u.activities.Pop()
	log.Print("vvvvv 4 :-", u.activities)
	xe = u.activities.FromDB("single", xx)
	if len(xe) > 0 {
		t.Error("simpleg.Set Failed Get user Activity from DB: ", xe)
	}
	log.Print("vvvvv 5 :-", u.activities)
	u.activities.Clear()
	xe = u.activities.FromDB("last", 8)
	if len(xe) > 0 {
		t.Error("simpleg.Set Failed Get user Activities from DB: ", xe)
	}
	log.Print("vvvvv 6 :-", u.activities)
	u.activities.Clear()
	xe = u.activities.FromDB("list", 2, 10)
	if len(xe) > 0 {
		t.Error("simpleg.Set Failed Get user Activities from DB: ", xe)
	}
	log.Print("vvvvv 7 :-", u.activities)

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

	uu.ID = uint64(4)
	uu.firstName = "Femisola"
	uu.lastName = "Adebara"
	uu.email = "timmytune002@gmail.com"
	uu.age = int64(12)
	ret = db.Set("save.object", "User", uu)
	if len(ret.Errors) != 0 {
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

	time.Sleep(time.Second * 1)

	ret = db.Delete("delete.object.field", "User", uint64(2), "lastName")
	if len(ret.Errors) != 0 {
		t.Error("simpleg.Delete lastName Failed Test got:", ret)
	}

	time.Sleep(time.Second * 1)

	retGet := db.Get("object.single", "User", uint64(2))
	if len(retGet.Errors) > 0 {
		t.Error("simpleg.Set Failed Test get user:", retGet)
	}
	user, ok := retGet.Data.(User)
	if !ok {
		t.Error("simpleg.Set Failed Test get user: convert ret to User", retGet)
	} else {
		log.Print("qqqqqqqq ", user.lastName)
	}

	//Get preexisting objects
	//rand.Seed(time.Now().UnixNano())

	var w sync.WaitGroup
	start = time.Now()
	for i := 1; i < 10; i++ {
		w.Add(1)
		go func() {
			defer w.Done()
			retGet := db.Get("object.single", "User", uint64(2))
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

	time.Sleep(2 * time.Second)
	//-----------------------------------------Get objects with raw query
	start = time.Now()
	q := db.Query()
	n := NodeQuery{}
	n.Name("da").Object("User").Q("age", ">=", int64(7)).Q("activities", "count-l", 15).Q("activities", "count-g", 2).Order("ID", "dsc").Limit(100)
	q.Do("object", n)
	retGet = q.Return("array", "da")
	if len(retGet.Errors) > 0 {
		t.Error("simpleg.Query Failed Test get users:", retGet.Errors)
	} else {
		log.Print("---- 4444 ---- ", len(retGet.Data.([]interface{})))
	}

	q = db.Query()
	n = NodeQuery{}
	n.Name("da").Object("User").Q("age", ">=", int64(7)).Q("activities", "date <", time.Now()).Order("ID", "dsc").Limit(100)
	q.Do("object", n)
	retGet = q.Return("array", "da")
	if len(retGet.Errors) > 0 {
		t.Error("simpleg.Query Failed Test get users:", retGet.Errors)
	} else {
		log.Print("---- 4444.1111 ---- ", len(retGet.Data.([]interface{})))
		Log.Print("---- 4444.1111 ---- ", retGet.Data)
	}

	q = db.Query()
	n = NodeQuery{}
	n.Name("da").Object("User").Q("age", ">=", int64(7)).Order("ID", "dsc").Limit(100).Q("active", "==", true)
	q.Do("object", n)
	retGet = q.Return("array", "da")
	if len(retGet.Errors) > 0 {
		t.Error("simpleg.Query Failed Test get users:", retGet.Errors)
	} else {
		log.Print("---- 4444.5555 ---- ", len(retGet.Data.([]interface{})))
		Log.Print("---- 4444.5555 ---- ", retGet.Data)
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

	time.Sleep(1 * time.Second)

	// prefix := []byte(db.Options.DBName)
	// opt := badger.DefaultIteratorOptions
	// opt.Prefix = prefix
	// opt.PrefetchSize = 20
	// opt.PrefetchValues = false
	// txn := db.KV.DB.NewTransaction(false)
	// defer txn.Discard()
	// iterator := txn.NewIterator(opt)
	// brk := false
	// iterator.Seek(prefix)
	// for !brk {
	// 	if !iterator.ValidForPrefix(prefix) {
	// 		iterator.Close()
	// 		break
	// 	}
	// 	item := iterator.Item()
	// 	var key []byte
	// 	key = item.KeyCopy(key)
	// 	kArray := bytes.Split(key, []byte(db.KV.D))
	// 	f, _ := binary.Uvarint(kArray[3])
	// 	t, _ := binary.Uvarint(kArray[4])
	// 	log.Printf("%s %s %s %s", string(kArray[1]), string(kArray[2]), strconv.Itoa(int(f)), strconv.Itoa(int(t)))
	// 	Log.Print(string(key))
	// 	iterator.Next()
	// }

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
		if (i + 1) != (9 - i + 1) {
			r := db.Set("save.link", "Friend", f)
			if len(r.Errors) > 0 {
				Log.Print(f, "//////////////////////////////////////////////////////////")
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
	ret2 := db.Get("link.single", "Friend", "->", uint64(1), uint64(10))
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

	time.Sleep(1 * time.Second)
	laq := NodeQuery{}
	la := db.Query()
	laq.Name("da").Link("Friend", "->").Q("FROM", "==", uint64(2))
	la.Do("link", laq)
	lar := la.Return("array", "da")
	if len(lar.Errors) > 0 {
		Log.Printf("---- Test Get Link had the following errors %s", lar.Errors)
		t.Error("simpleg.Get Failed Test get Friend:", lar)
	}
	lad, ok := lar.Data.([]interface{})
	if !ok {
		t.Error("simpleg.Get Failed Test get Friend wrong data interface conversion:", lar.Data)
	}
	l, ok := lad[0].(Friend)
	if !ok {
		t.Error("simpleg.Get Failed Test get Friend wrong data interface conversion:", la)
	}
	xx, xe = l.addActivity(time.Now(), "windows-10", uint64(1))
	xx, xe = l.addActivity(time.Now(), "xp-360", uint64(2))
	xx, xe = l.addActivity(time.Now(), "android", uint64(3))
	xx, xe = l.addActivity(time.Now(), "ios", uint64(4))
	xx, xe = l.addActivity(time.Now(), "symbian", uint64(5))
	xx, xe = l.addActivity(time.Now(), "windows-phone", uint64(6))
	xx, xe = l.addActivity(time.Now(), "linux-phone", uint64(7))
	xx, xe = l.addActivity(time.Now(), "zte", uint64(8))
	xx, xe = l.addActivity(time.Now(), "nokia", uint64(100))
	log.Print("mmmm 1 :-", xx, xe)
	if len(xe) > 0 {
		t.Error("simpleg.Set Failed Add user Activity: ", xx, xe)
	}
	xe = l.activities.Save()
	if len(xe) > 0 {
		t.Error("simpleg.Set Failed Save user Activity: ", xe)
	}

	time.Sleep(1 * time.Second)

	l.activities.Clear()
	log.Print("rrrrr 111111", l.activities)
	errs := l.activities.FromDB("last", 10)
	if len(errs) > 0 {
		t.Error("Failed to load Friend Activities: ", errs)
	}
	log.Print("rrrrr 222222", l.activities)
	errs = l.activities.Delete(uint64(7), "deviceID")
	if len(errs) > 0 {
		t.Error("Failed to delete Activities field: ", errs)
	}
	errs = l.activities.Delete(uint64(6), "")
	if len(errs) > 0 {
		t.Error("Failed to delete Activities field: ", errs)
	}

	time.Sleep(1 * time.Second)
	l.activities.Clear()
	log.Print("rrrrr 333333", l.activities)
	errs = l.activities.FromDB("last", 10)
	if len(errs) > 0 {
		t.Error("Failed to load Friend Activities: ", errs)
	}
	log.Print("rrrrr 44444", l.activities)

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

	nl := NodeQuery{}
	nl.Name("da").Link("Like", "<-")
	ql := db.Query()
	ql.Do("link", nl)
	rl := ql.Return("array", "da")
	if len(rl.Errors) > 0 {
		t.Error("simpleg.Set Failed Test getLikes Like:", rl)
	} else {
		log.Print(rl.Data)
	}

	nl = NodeQuery{}
	nl.Name("da").Link("Like", "->")
	ql = db.Query()
	ql.Do("link", nl)
	rl = ql.Return("array", "da")
	if len(rl.Errors) > 0 {
		t.Error("simpleg.Set Failed Test getLikes Like:", rl)
	} else {
		log.Print(rl.Data)
	}

	// prefix := []byte(db.Options.DBName + db.KV.D + "a")
	// opt := badger.DefaultIteratorOptions
	// opt.Prefix = prefix
	// opt.PrefetchSize = 20
	// opt.PrefetchValues = false
	// txn := db.KV.DB.NewTransaction(false)
	// defer txn.Discard()
	// iterator := txn.NewIterator(opt)
	// //defer iterator.Close()
	// brk := false

	// iterator.Seek(prefix)
	// for !brk {
	// 	if !iterator.ValidForPrefix(prefix) {
	// 		iterator.Close()
	// 		break
	// 	}
	// 	item := iterator.Item()
	// 	var key []byte
	// 	key = item.KeyCopy(key)
	// 	kArray := bytes.Split(key, []byte(db.KV.D))
	// 	//f, _ := binary.Uvarint(kArray[3])
	// 	//t, _ := binary.Uvarint(kArray[4])
	// 	//log.Printf("%s %s %s %s", string(kArray[1]), string(kArray[2]), strconv.Itoa(int(f)), strconv.Itoa(int(t)))
	// 	log.Print(string(key), kArray[4])
	// 	iterator.Next()
	// }

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
		f.accepted = true
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
		f2.accepted = true
		if 10 != (9-i+1) && 0 != (9-i+1) {
			r := db.Set("save.link", "Friend", f2)
			if len(r.Errors) > 0 {
				t.Error("simpleg.Set Failed Test set Friend:", r)
			}
		}
		time.Sleep(1 * time.Second)
	}

	time.Sleep(1 * time.Second)

	ret = db.Delete("delete.link.field", "Friend", uint64(2), uint64(7), "accepted")
	if len(ret.Errors) != 0 {
		t.Error("simpleg.Delete accepted Failed Test got:", ret)
	}

	ret = db.Delete("delete.link", "Friend", uint64(2), uint64(6))
	if len(ret.Errors) != 0 {
		t.Error("simpleg.Delete accepted Failed Test got:", ret)
	}

	ret = db.Delete("delete.object", "User", uint64(4))
	if len(ret.Errors) != 0 {
		t.Error("simpleg.Delete Object Failed Test got:", ret)
	}
	time.Sleep(2 * time.Second)

	n6 := NodeQuery{}
	q6 := db.Query()
	n6.Name("da").Link("Friend", "->").Limit(5).Q("FROM", "==", uint64(2)).Q("TO", "==", uint64(7))
	q6.Do("link", n6)
	ret6 := q6.Return("single", "da", 0)
	if len(ret6.Errors) > 0 {
		t.Error("simpleg.Get Failed Test get Friend:", ret6.Errors)
	} else {
		friend := ret6.Data.(Friend)
		log.Print("---- 4.4.4.4 ---- ", friend.accepted)
	}

	n6 = NodeQuery{}
	q6 = db.Query()
	n6.Name("da").Link("Friend", "->").Limit(5).Q("FROM", "==", uint64(2))
	q6.Do("link", n6)
	ret6 = q6.Return("array", "da")
	if len(ret6.Errors) > 0 {
		t.Error("simpleg.Get Failed Test get Friend:", ret6.Errors)
	} else {
		log.Print("---- 5.5.5.5 ---- ", len(ret6.Data.([]interface{})))
	}

	n6 = NodeQuery{}
	q6 = db.Query()
	n6.Name("da").Link("Friend", "->").Q("activities", "count-g", 2)
	q6.Do("link", n6)
	ret6 = q6.Return("array", "da")
	if len(ret6.Errors) > 0 {
		t.Error("simpleg.Get Failed Test get Friend:", ret6.Errors)
	} else {
		log.Print("---- 6.6.6.6 ---- ", len(ret6.Data.([]interface{})))
	}

	n7 := NodeQuery{}
	q7 := db.Query()
	n7.Name("da").Link("Friend", "->").Limit(100).Q("FROM", "==", uint64(10))
	q7.Do("link", n7)
	ret7 := q7.Return("array", "da")
	if len(ret7.Errors) > 0 {
		t.Error("simpleg.Get Failed Test get Friend:", ret7.Errors)
	} else {
		log.Print("---- 6666 ---- ", len(ret7.Data.([]interface{})))
	}

	startUser1 := NodeQuery{}
	startUser1.Object("User").Name("first").Q("age", ">", int64(17))

	friends1 := NodeQuery{}
	friends1.Link("Friend", "-").Name("link")

	mutual1 := NodeQuery{}
	mutual1.Object("User").Q("ID", "==", uint64(2)).Name("mutual")

	query1 := db.Query()
	query1.Do("graph.p", startUser1, friends1, mutual1)
	retQuery1 := query1.Return("map", "mutual", "link", "first")
	if len(retQuery1.Errors) > 0 {
		Log.Printf("---- Test Get Link had the following errors %s", retQuery1.Errors)
		t.Error("simpleg.Get Failed Test get Friend:", retQuery1)
	} else {
		//log.Print("---- 8888.1 ---- ", retQuery.Data)
		log.Print("---- 8888.1 ---- ", len(retQuery1.Data.(map[string]interface{})["mutual"].([]interface{})))
		log.Print("---- 8888.2 ---- ", len(retQuery1.Data.(map[string]interface{})["link"].([]interface{})))
		log.Print("---- 8888.3 ---- ", len(retQuery1.Data.(map[string]interface{})["first"].([]interface{})))
	}

	startUser1 = NodeQuery{}
	startUser1.Object("User").Name("first").Q("age", ">", int64(17))

	friends1 = NodeQuery{}
	friends1.Link("Friend", "-").Name("link")

	mutual1 = NodeQuery{}
	mutual1.Object("User").Q("ID", "==", uint64(2)).Name("mutual")

	query1 = db.Query()
	query1.Do("graph.p", mutual1, friends1, startUser1)
	retQuery1 = query1.Return("map", "mutual", "link", "first")
	if len(retQuery1.Errors) > 0 {
		Log.Printf("---- Test Get Link had the following errors %s", retQuery1.Errors)
		t.Error("simpleg.Get Failed Test get Friend:", retQuery1)
	} else {
		//log.Print("---- 8888.1 ---- ", retQuery.Data)
		log.Print("---- 8888.1 ---- ", len(retQuery1.Data.(map[string]interface{})["mutual"].([]interface{})))
		log.Print("---- 8888.2 ---- ", len(retQuery1.Data.(map[string]interface{})["link"].([]interface{})))
		log.Print("---- 8888.3 ---- ", len(retQuery1.Data.(map[string]interface{})["first"].([]interface{})))
	}

	startUser := NodeQuery{}
	startUser.Object("User").Q("age", ">", int64(17)).Name("first")

	friends := NodeQuery{}
	friends.Link("Friend", "-").Name("link").Limit(100) //.Skip(10)

	mutual := NodeQuery{}
	mutual.Object("User").Name("mutual")

	friends2 := NodeQuery{}
	friends2.Link("Friend", "-")

	endUser := NodeQuery{}
	endUser.Object("User").Limit(5).Name("last") //.Q("ID", "==", uint64(10))

	query := db.Query()
	query.Do("graph.p", startUser, friends, mutual, friends2, endUser)
	retQuery := query.Return("map", "mutual", "link", "last", "first")
	if len(retQuery.Errors) > 0 {
		Log.Printf("---- Test Get Link had the following errors %s", retQuery.Errors)
		t.Error("simpleg.Get Failed Test get Friend:", retQuery)
	} else {
		//log.Print("---- 8888.1 ---- ", retQuery.Data)
		log.Print("---- 8888.5 ---- ", len(retQuery.Data.(map[string]interface{})["mutual"].([]interface{})))
		log.Print("---- 8888.6 ---- ", len(retQuery.Data.(map[string]interface{})["link"].([]interface{})))
		log.Print("---- 8888.7 ---- ", len(retQuery.Data.(map[string]interface{})["first"].([]interface{})))
		log.Print("---- 8888.8 ---- ", len(retQuery.Data.(map[string]interface{})["last"].([]interface{})))
	}

	startUser = NodeQuery{}
	startUser.Object("User").Q("date", "<", time.Now()).Name("first")

	friends = NodeQuery{}
	friends.Link("Friend", "-").Name("link").Limit(100) //.Skip(10)

	mutual = NodeQuery{}
	mutual.Object("User").Name("mutual")

	friends2 = NodeQuery{}
	friends2.Link("Friend", "<-")

	endUser = NodeQuery{}
	endUser.Object("User").Q("ID", "==", uint64(10)).Limit(100).Name("last")

	query = db.Query()
	query.Do("graph.p", endUser, friends2, mutual, friends, startUser)
	retQuery = query.Return("map", "mutual", "link", "last", "first")
	if len(retQuery.Errors) > 0 {
		Log.Printf("---- Test Get Link had the following errors %s", retQuery.Errors)
		t.Error("simpleg.Get Failed Test get Friend:", retQuery)
	} else {
		//log.Print("---- 8888.1 ---- ", retQuery.Data)
		log.Print("---- 8888.1 ---- ", len(retQuery.Data.(map[string]interface{})["mutual"].([]interface{})))
		log.Print("---- 8888.2 ---- ", len(retQuery.Data.(map[string]interface{})["link"].([]interface{})))
		log.Print("---- 8888.3 ---- ", len(retQuery.Data.(map[string]interface{})["first"].([]interface{})))
		log.Print("---- 8888.4 ---- ", len(retQuery.Data.(map[string]interface{})["last"].([]interface{})))
	}

	startUser = NodeQuery{}
	startUser.Object("User").Q("age", ">", int64(17)).Name("first").Limit(5).Order("ID", "dsc")

	friends = NodeQuery{}
	friends.Link("Friend", "-").Name("link").Limit(100) //.Skip(10)

	mutual = NodeQuery{}
	mutual.Object("User").Name("mutual").Q("ID", "==", uint64(9))

	friends2 = NodeQuery{}
	friends2.Link("Friend", "-").Name("link2")

	endUser = NodeQuery{}
	endUser.Object("User").Q("ID", "==", uint64(2)).Limit(100).Name("last")

	likes := NodeQuery{}
	likes.Link("Like", "->").Limit(100).Name("like")

	post := NodeQuery{}
	post.Object("Post").Limit(100).Name("post") //.Q("ID", "==", uint64(19))

	query = db.Query()
	query.Do("graph.s", endUser, friends2, mutual, friends, startUser, likes, post)

	retQuery = query.Return("map", "last", "link2", "mutual", "link", "first", "like", "post")
	if len(retQuery.Errors) > 0 {
		Log.Printf("---- Test Get Link had the following errors %s", retQuery.Errors)
		t.Error("simpleg.Get Failed Test get Friend:", retQuery)
	} else {
		//log.Print("---- 8888.1 ---- ", retQuery.Data)
		log.Print("---- 9999.1 ---- ", len(retQuery.Data.(map[string]interface{})["last"].([]interface{})))
		log.Print("---- 9999.2 ---- ", len(retQuery.Data.(map[string]interface{})["link2"].([]interface{})))
		log.Print("---- 9999.3 ---- ", len(retQuery.Data.(map[string]interface{})["mutual"].([]interface{})))
		log.Print("---- 9999.4 ---- ", len(retQuery.Data.(map[string]interface{})["link"].([]interface{})))
		log.Print("---- 9999.5 ---- ", len(retQuery.Data.(map[string]interface{})["first"].([]interface{})))
		//log.Print("---- 9999.5.1 -- ", retQuery.Data.(map[string]interface{})["first"].([]interface{}))
		log.Print("---- 9999.6 ---- ", len(retQuery.Data.(map[string]interface{})["like"].([]interface{})))
		log.Print("---- 9999.7 ---- ", len(retQuery.Data.(map[string]interface{})["post"].([]interface{})))
	}

	liks := NodeQuery{}
	liks.Link("Like", "<-").Limit(100).Name("like")

	query = db.Query()
	query.Do("graph.s", post, liks, startUser, friends, mutual, friends2, endUser)

	retQuery = query.Return("map", "last", "link2", "mutual", "link", "first", "like", "post")
	if len(retQuery.Errors) > 0 {
		Log.Printf("---- Test Get Link had the following errors %s", retQuery.Errors)
		t.Error("simpleg.Get Failed Test get Friend:", retQuery)
	} else {
		//log.Print("---- 8888.1 ---- ", retQuery.Data)
		log.Print("---- aaaa.1 ---- ", len(retQuery.Data.(map[string]interface{})["last"].([]interface{})))
		log.Print("---- aaaa.2 ---- ", len(retQuery.Data.(map[string]interface{})["link2"].([]interface{})))
		log.Print("---- aaaa.3 ---- ", len(retQuery.Data.(map[string]interface{})["mutual"].([]interface{})))
		log.Print("---- aaaa.4 ---- ", len(retQuery.Data.(map[string]interface{})["link"].([]interface{})))
		log.Print("---- aaaa.5 ---- ", len(retQuery.Data.(map[string]interface{})["first"].([]interface{})))
		//log.Print("---- 9999.5.1 -- ", retQuery.Data.(map[string]interface{})["first"].([]interface{}))
		log.Print("---- aaaa.6 ---- ", len(retQuery.Data.(map[string]interface{})["like"].([]interface{})))
		//log.Print("---- aaaa.6.1 -- ", retQuery.Data.(map[string]interface{})["like"].([]interface{}))
		log.Print("---- aaaa.7 ---- ", len(retQuery.Data.(map[string]interface{})["post"].([]interface{})))
	}

	likes5 := NodeQuery{}
	likes5.Link("Like", "<-").Limit(100).Name("like")

	query = db.Query()
	query.Do("graph.s", post, likes5, startUser, friends, mutual, friends2, endUser)

	retQuery = query.Return("map", "last", "link2", "mutual", "link", "first", "like", "post")
	if len(retQuery.Errors) > 0 {
		Log.Printf("---- Test Get Link had the following errors %s", retQuery.Errors)
		t.Error("simpleg.Get Failed Test get Friend:", retQuery)
	} else {
		log.Print("---- bbbb.1 ---- ", len(retQuery.Data.(map[string]interface{})["last"].([]interface{})))
		log.Print("---- bbbb.2 ---- ", len(retQuery.Data.(map[string]interface{})["link2"].([]interface{})))
		log.Print("---- bbbb.3 ---- ", len(retQuery.Data.(map[string]interface{})["mutual"].([]interface{})))
		log.Print("---- bbbb.4 ---- ", len(retQuery.Data.(map[string]interface{})["link"].([]interface{})))
		log.Print("---- bbbb.5 ---- ", len(retQuery.Data.(map[string]interface{})["first"].([]interface{})))
		log.Print("---- bbbb.6 ---- ", len(retQuery.Data.(map[string]interface{})["like"].([]interface{})))
		log.Print("---- bbbb.7 ---- ", len(retQuery.Data.(map[string]interface{})["post"].([]interface{})))
	}

	startUser = NodeQuery{}
	startUser.Object("User").Name("first") //.Q("activities", "count-g", 2)

	friends = NodeQuery{}
	friends.Link("Friend", "-").Name("link").Limit(100) //.Skip(10)

	mutual = NodeQuery{}
	mutual.Object("User").Name("mutual").Q("ID", "==", uint64(9))

	friends2 = NodeQuery{}
	friends2.Link("Friend", "-").Name("link2")

	endUser = NodeQuery{}
	endUser.Object("User").Q("activities", "count-g", 2).Limit(100).Name("last")

	likes = NodeQuery{}
	likes.Link("Like", "->").Limit(100).Name("like")

	post = NodeQuery{}
	post.Object("Post").Limit(100).Name("post") //.Q("ID", "==", uint64(19))

	query = db.Query()
	query.Do("graph.s", startUser, friends2, mutual, friends, endUser, likes, post)

	retQuery = query.Return("map", "last", "link2", "mutual", "link", "first", "like", "post")
	if len(retQuery.Errors) > 0 {
		Log.Printf("---- Test Get Link had the following errors %s", retQuery.Errors)
		t.Error("simpleg.Get Failed Test get Friend:", retQuery)
	} else {
		//log.Print("---- 8888.1 ---- ", retQuery.Data)
		log.Print("---- cccc.1 ---- ", len(retQuery.Data.(map[string]interface{})["last"].([]interface{})))
		log.Print("---- cccc.2 ---- ", len(retQuery.Data.(map[string]interface{})["link2"].([]interface{})))
		log.Print("---- cccc.3 ---- ", len(retQuery.Data.(map[string]interface{})["mutual"].([]interface{})))
		log.Print("---- cccc.4 ---- ", len(retQuery.Data.(map[string]interface{})["link"].([]interface{})))
		log.Print("---- cccc.5 ---- ", len(retQuery.Data.(map[string]interface{})["first"].([]interface{})))
		log.Print("---- cccc.6 ---- ", len(retQuery.Data.(map[string]interface{})["like"].([]interface{})))
		log.Print("---- cccc.7 ---- ", len(retQuery.Data.(map[string]interface{})["post"].([]interface{})))
	}

	n6 = NodeQuery{}
	q6 = db.Query()
	n6.Name("da").Link("Friend", "->").Q("FROM", "==", uint64(2)).Q("TO", "==", uint64(6))
	q6.Do("link", n6)
	ret6 = q6.Return("single", "da", 0)
	if len(ret6.Errors) == 0 {
		t.Error("simpleg.Get Failed Test get Friend:", ret6.Data)
	} else {
		log.Print("???????????", ret6.Errors)
	}

	n6 = NodeQuery{}
	q6 = db.Query()
	n6.Name("da").Object("User").Q("ID", "==", uint64(4))
	q6.Do("object", n6)
	ret6 = q6.Return("single", "da", 0)
	if len(ret6.Errors) == 0 {
		t.Error("simpleg.Get Failed Test get User:", ret6.Data)
	} else {
		log.Print("???????????", ret6.Errors)
	}

	n6 = NodeQuery{}
	q6 = db.Query()
	n6.Name("da").Object("User").Q("ID", "==", uint64(2))
	q6.Do("errored")
	q6.Do("object", n6)
	ret6 = q6.Return("single", "da", 0)
	if len(ret6.Errors) == 0 {
		t.Error("simpleg.Get Failed Test get User:", ret6.Data)
	} else {
		log.Print("???????????", ret6.Errors)
	}

	n6 = NodeQuery{}
	q6 = db.Query()
	n6.Name("da").Object("User")
	q6.Do("object", n6)
	q6.Do("return.map", "array", "da")
	ret6 = q6.Return("skip")
	if len(ret6.Errors) != 0 {
		t.Error("simpleg.Get Failed Test get Users fro map return:", ret6.Errors)
	} else {
		Log.Print(ret6.Data)
		log.Print("||||||||| ", len(ret6.Data.([]map[string]interface{})))
	}

	u1 := NodeQuery{}
	u1.Name("u1").Object("User").Q("ID", "==", uint64(2))

	u2 := NodeQuery{}
	u2.Name("u2").Object("User").Q("ID", "==", uint64(5))

	friends = NodeQuery{}
	friends.Link("Friend", "-").Name("link").Limit(100) //.Skip(10)

	mutual = NodeQuery{}
	mutual.Object("User").Name("mutual")

	friends2 = NodeQuery{}
	friends2.Link("Friend", "-").Name("link2")

	endUser = NodeQuery{}
	endUser.Object("User").Q("activities", "count-g", 2).Limit(100).Name("last")

	likes = NodeQuery{}
	likes.Link("Like", "->").Limit(100).Name("like")

	post = NodeQuery{}
	post.Object("Post").Limit(100).Name("post") //.Q("ID", "==", uint64(19))

	query = db.Query()
	query.Do("object", u1)
	query.Do("object", u2)
	query.Do("merge.objectlist", "u1", "u2", "ua")
	query.Do("graph.p.o", "ua", friends2, mutual, friends, endUser, likes, post)
	query.Do("return.map", "map", "ua", "last", "link2", "mutual", "link", "like", "post")
	retQuery = query.Return("skip")
	if len(retQuery.Errors) > 0 {
		Log.Printf("---- Test Get Link had the following errors %s", retQuery.Errors)
		t.Error("simpleg.Get Failed Test get Friend:", retQuery.Errors)
	} else {
		log.Print("---- dddd.5 ---- ", len(retQuery.Data.(map[string]interface{})["ua"].([]map[string]interface{})))
		log.Print("---- dddd.2 ---- ", len(retQuery.Data.(map[string]interface{})["link2"].([]map[string]interface{})))
		log.Print("---- dddd.3 ---- ", len(retQuery.Data.(map[string]interface{})["mutual"].([]map[string]interface{})))
		log.Print("---- dddd.4 ---- ", len(retQuery.Data.(map[string]interface{})["link"].([]map[string]interface{})))
		log.Print("---- dddd.1 ---- ", len(retQuery.Data.(map[string]interface{})["last"].([]map[string]interface{})))
		log.Print("---- dddd.6 ---- ", len(retQuery.Data.(map[string]interface{})["like"].([]map[string]interface{})))
		log.Print("---- dddd.7 ---- ", len(retQuery.Data.(map[string]interface{})["post"].([]map[string]interface{})))
	}

	users1 := NodeQuery{}
	users1.Name("users1").Object("User").SetFields("firstName.email")

	isFriend := NodeQuery{}
	isFriend.Link("Friend", "-").Name("isFriend").Limit(100) //.Skip(10)

	users2 := NodeQuery{}
	users2.Object("User").Name("users2").SetFields("firstName")

	isFriend2 := NodeQuery{}
	isFriend2.Link("Friend", "-").Name("isFriend2")

	users3 := NodeQuery{}
	users3.Object("User").Q("activities", "count-g", 2).Limit(100).Name("users3")

	users4 := NodeQuery{}
	users4.Name("users4").Object("User").SetFields("firstName.email")

	query = db.Query()
	query.Do("object", users1)
	query.Do("object", users4)
	query.Do("graph.p.o", "users1", isFriend, users2)
	query.Do("embeded", "emb", "users1", "User", isFriend, users2)
	query.Do("embeded", "emb2", "users4", "User", isFriend, users2, isFriend2, users1)
	query.Do("return.map", "map", "users1", "emb2")
	retQuery = query.Return("skip")
	if len(retQuery.Errors) > 0 {
		Log.Printf("---- Test Get Link had the following errors %s", retQuery.Errors)
		t.Error("simpleg.Get Failed Test get Friend:", retQuery.Errors)
	} else {
		//log.Print("---- eeee.5 ---- ", len(retQuery.Data.(map[string]interface{})["users1"].([]map[string]interface{})))
		//log.Print("---- eeee.2 ---- ", len(retQuery.Data.(map[string]interface{})["isFriend"].([]map[string]interface{})))
		//log.Print("---- eeee.3 ---- ", len(retQuery.Data.(map[string]interface{})["users2"].([]map[string]interface{})))
		m := retQuery.Data.(map[string]interface{})

		Log.Print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$", m["emb2"])
		Log.Print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$", m["users1"])
	}

	// prefix := []byte(db.Options.DBName + db.KV.D + "User" + db.KV.D + "date")
	// opt := badger.DefaultIteratorOptions
	// opt.Prefix = prefix
	// opt.PrefetchSize = 20
	// opt.PrefetchValues = false
	// txn := db.KV.DB.NewTransaction(false)
	// defer txn.Discard()
	// iterator := txn.NewIterator(opt)
	// //defer iterator.Close()
	// brk := false

	// iterator.Seek(prefix)
	// for !brk {
	// 	if !iterator.ValidForPrefix(prefix) {
	// 		iterator.Close()
	// 		break
	// 	}
	// 	item := iterator.Item()
	// 	var key []byte
	// 	key = item.KeyCopy(key)
	// 	kArray := bytes.Split(key, []byte(db.KV.D))
	// 	//f, _ := binary.Uvarint(kArray[3])
	// 	//t, _ := binary.Uvarint(kArray[4])
	// 	//log.Printf("%s %s %s %s", string(kArray[1]), string(kArray[2]), strconv.Itoa(int(f)), strconv.Itoa(int(t)))
	// 	log.Print(string(key), string(kArray[4]))
	// 	iterator.Next()
	// }

	q = db.Query()
	n = NodeQuery{}
	n.Name("da").Object("User").Q("date", "<", time.Now()).Limit(100)
	//n.Name("da").Object("User").Q("firstName", "prefix", "yin").Limit(100)
	q.Do("object", n)
	retGet = q.Return("array", "da")
	if len(retGet.Errors) > 0 {
		t.Error("simpleg.Query Failed Test get users:", retGet.Errors)
	} else {
		log.Print("---- date ---- ", len(retGet.Data.([]interface{})))
	}

	err := db.Backup()
	if err != nil {
		log.Print("---- backup error ---- ", err)
		t.Error("simpleg.Backup:", err)
	}

	err = db.Restore("1ggirod_5966_6316")
	if err != nil {
		log.Print("---- restore error ---- ", err)
		t.Error("simpleg.Restore:", err)
	}

}
