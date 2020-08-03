package keyvalue

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v2"
)

type BatchReader struct {
	input    chan WriterData
	shotdown bool
	kv       *KV
	lock     sync.Mutex
}

func (b *BatchReader) Write(k1, k2, k3, k4, k5 string, value []byte) {
	var buffer bytes.Buffer
	buffer.WriteString(k1)
	buffer.WriteString(b.kv.D)
	buffer.WriteString(k2)
	buffer.WriteString(b.kv.D)
	buffer.WriteString(k3)
	buffer.WriteString(b.kv.D)
	buffer.WriteString(k4)
	if k5 != "" {
		buffer.WriteString(b.kv.D)
		buffer.WriteString(k5)
	}
	b.input <- WriterData{buffer.Bytes(), value, true}
}

func (b *BatchReader) Delete(key ...string) {
	b.input <- WriterData{Key: []byte(strings.Join(key, b.kv.D)), Type: false}
}

func (b *BatchReader) Go(inputLength int, db *badger.DB, kv *KV, length int) {
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

}
func (b *BatchReader) mainRoutine(id int, input chan WriterData, internal chan bool, db *badger.DB) {
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

func (b *BatchReader) childRoutine(chs []chan bool) {
	defer func() {
		r := recover()
		if r != nil {
			fmt.Println("Recovered in BatchWriter.childRoutine ", r)
			b.childRoutine(chs)
		}

	}()
	for {
		time.Sleep(1 * time.Second)
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
