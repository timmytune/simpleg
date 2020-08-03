package keyvalue

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v2"
)

type WriterData struct {
	Key   []byte
	Value []byte
	Type  bool
}

type BatchWriter struct {
	input    chan WriterData
	shotdown bool
	kv       *KV
	lock     sync.Mutex
}

func GetWriteData(k1, k2, k3, k4, k5 string, value []byte, seprator string) WriterData {
	data := k1 + seprator + k2 + seprator + k3 + seprator + k4
	if k5 != "" {
		data = data + seprator + k5
	}

	return WriterData{[]byte(data), value, true}
}

func (b *BatchWriter) Write(k1, k2, k3, k4, k5 string, value []byte) {
	var buffer bytes.Buffer
	//b.lock.Lock()
	seperator := b.kv.D
	//b.lock.Unlock()
	buffer.WriteString(k1)
	buffer.WriteString(seperator)
	buffer.WriteString(k2)
	buffer.WriteString(seperator)
	buffer.WriteString(k3)
	buffer.WriteString(seperator)
	buffer.WriteString(k4)
	if k5 != "" {
		buffer.WriteString(seperator)
		buffer.WriteString(k5)
	}
	//b.lock.Lock()
	b.input <- WriterData{buffer.Bytes(), value, true}
	//b.lock.Unlock()

}

func (b *BatchWriter) Delete(key ...string) {
	b.input <- WriterData{Key: []byte(strings.Join(key, b.kv.D)), Type: false}
}

func (b *BatchWriter) Go(inputLength int, db *badger.DB, kv *KV, length int) chan WriterData {
	b.lock = sync.Mutex{}
	internal := make([]chan bool, length)
	input := make(chan WriterData, inputLength)
	for i := 0; i < length; i++ {
		internal[i] = make(chan bool)
		go b.mainRoutine(i, input, internal[i], db)
	}
	go b.childRoutine(internal)
	b.input = input
	b.kv = kv
	return input
}
func (b *BatchWriter) mainRoutine(id int, input chan WriterData, internal chan bool, db *badger.DB) {
	defer func() {
		r := recover()
		fmt.Println("Recovered in BatchWriter.mainRoutine ", r)
		b.mainRoutine(id, input, internal, db)
	}()
	var transaction *badger.Txn
	var transactionCount int
	for {
		select {
		case data, ok := <-input:
			if transaction == nil {
				transaction = db.NewTransaction(true)
			}
			err := transaction.Set(data.Key, data.Value)
			transactionCount++
			if err != nil {
				transaction.Commit()
				transaction = db.NewTransaction(true)
				if data.Type {
					transaction.Set(data.Key, data.Value)
				} else {
					transaction.Delete(data.Key)
				}
				transactionCount = 1
			}
			if !ok && len(input) == 0 {
				b.lock.Lock()
				b.shotdown = true
				b.lock.Unlock()
			}
		case _, ok := <-internal:
			if transactionCount > 0 && len(input) == 0 {
				transaction.Commit()
				transaction = db.NewTransaction(true)
				transactionCount = 0
			}
			if !ok {
				if transaction != nil {
					transaction.Commit()
				}
				return
			}
		}
	}
}

func (b *BatchWriter) childRoutine(chs []chan bool) {
	defer func() {
		r := recover()
		if r != nil {
			fmt.Println("Recovered in BatchWriter.childRoutine ", r)
			b.childRoutine(chs)
		}

	}()
	for {
		time.Sleep(100 * time.Millisecond)
		b.lock.Lock()
		shotdown := b.shotdown
		b.lock.Unlock()
		for _, ch := range chs {
			if shotdown {
				ch <- false
			} else {
				ch <- true
			}
		}
		if shotdown {
			return
		}
	}
}

func (b *BatchWriter) Close() {
	b.lock.Lock()
	b.shotdown = true
	b.lock.Unlock()
}
