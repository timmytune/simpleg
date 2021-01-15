package keyvalue

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

var s *KV

func TestMain(m *testing.M) {
	// set up database for tests
	kd := GetDefaultKVOptions()
	kd.D = "+"
	bd := BadgerDefaultOptions("/data/badgerTest")
	bd.Truncate = true
	k, err := Open(kd, bd)
	s = k
	if err != nil {
		log.Fatal(err)
	}

	ret := m.Run()
	//time.Sleep(5 * time.Minute)
	s.Close()
	os.RemoveAll("/data/badgerTest")

	os.Exit(ret)
}

func TestSet(t *testing.T) {
	// should be able to add a key and value
	if err := s.Set("key", "key2", "key3", "key4", "key5", []byte("yinka")); err != nil {
		t.Error("Set operation failed:", err)
	}
}

func TestGetNextID(t *testing.T) {
	// should be able to add a key and value
	if id, err := s.GetNextID("user", 100); err != nil {
		t.Error("Set operation failed:", err)
	} else {
		log.Print("ID gotten from getID -- ", int(id))
	}

	if id, err := s.GetNextID("user", 20); err != nil {
		t.Error("Set operation failed:", err)
	} else {
		log.Print("ID2 gotten from getID -- ", int(id))
	}

	if id, err := s.GetNextID("simi", 20); err != nil {
		t.Error("Set operation failed:", err)
	} else {
		log.Print("ID3 gotten from getID -- ", int(id))
	}
}

func TestWrite(t *testing.T) {
	//var wg sync.WaitGroup
	start := time.Now()
	// should be able to add a key and value
	if err := s.Write("key", "key2", "key3", "key4", "key6", []byte("adedoyin")); err != nil {
		t.Error("Set operation failed:", err)
	}
	if err := s.Write("key", "key2", "key3", "key4", "int", IntToBytes(500)); err != nil {
		t.Error("Set operation failed for number:", err)
	}

	for i := 0; i < 1; i++ {
		func() {
			//wg.Add(1)
			//defer wg.Done()
			if err := s.Write("key", "key2", "key3", "key4", strconv.Itoa(i), IntToBytes(i)); err != nil {
				t.Error("Set operation failed:", err)
			}
		}()
	}
	//wg.Wait()
	s.FlushWrites()

	elapsed := time.Since(start)
	log.Printf("Write took %s", elapsed)
}

func TestWrite2(t *testing.T) {
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Writer2.Write(IntToBytes(rand.Intn(10000000000)), "key9", "key2", "key3", "key4--", strconv.Itoa(rand.Intn(10000000000)))
			if err != nil {
				t.Error("Writer 2 operation failed:", err)
			}

		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	log.Printf("Write2 took %s", elapsed)
	time.Sleep(1 * time.Second)
}

func TestGet(t *testing.T) {
	// should be able to retrieve the value of a key
	start := time.Now()
	val, err := s.Get("key", "key2", "key3", "key4", "key5")
	elapsed := time.Since(start)
	log.Printf("Test get took %s", elapsed)
	if err != nil {
		t.Error("Get operation failed:", err)
	}
	// val should be "value"
	if string(val) != "yinka" {
		t.Error("Get: Expected value \"yinka\", got ", val)
	}

	val, err = s.Get("key", "key2", "key3", "key4", "key6")
	//log.Println(err)
	if err != nil {
		t.Error("Get operation failed:", err)
	}
	// val should be "value"

	if string(val) != "adedoyin" {
		t.Error("Get: Expected value \"adedoyin\", got ", val)
	}
}

func TestRead(t *testing.T) {
	// should be able to retrieve the value of a key
	start := time.Now()
	val, err := s.Read("key", "key2", "key3")
	elapsed := time.Since(start)
	log.Printf("Test Read took %s", elapsed)
	if len(err) > 0 {
		t.Error("Read operation had some errors:", err)
	}
	// val should be "value"
	if len(val) <= 0 {
		t.Error("Read: Expected value map with values, got ", val)
	}

	//log.Print(val)
}

func TestStream(t *testing.T) {
	start := time.Now()
	data, err := s.Stream([]string{"key9", "key2"}, nil)
	elapsed := time.Since(start)
	log.Print("LLLLLLLLLLLLLLLLLLLLLLLLL", data)
	if err != nil {
		t.Error("Set operation failed:", err)
	}
	log.Printf("Get stream took %s", elapsed)
	//log.Print(data)

}

func TestAddNum(t *testing.T) {
	start := time.Now()
	// should be able to add a key and value
	m := s.AddNum("key.key2.key3.key4.int")
	m.Add(IntToBytes(50))
	m.Add(IntToBytes(50))
	elapsed := time.Since(start)
	log.Printf("Get merge operator took %s", elapsed)
	n, err := m.Get()
	if err != nil {
		t.Error("Merge Operation failed")
	}
	log.Print(string(n))

	m.Stop()

}

func TestDelete(t *testing.T) {
	// should be able to delete key
	if err := s.Delete("key.key2.key3.key4.key5"); err != nil {
		t.Error("Delete operation failed:", err)
	}

	// key should be gone
	_, err := s.Get("key.key2.key3.key4.key5")
	if err == nil {
		t.Error("Key \"key\" should not be found, but it was")
	}
}

func TestDeletePrefix(t *testing.T) {
	// should be able to delete key
	if err := s.DeletePrefix("key."); err != nil {
		t.Error("DeletePrefix operation failed:", err)
	}
}
