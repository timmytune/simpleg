package keyvalue

import (
	"runtime/debug"
	"strings"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger"
)

type WriterData struct {
	Key   []byte
	Value []byte
	Type  bool
	err   chan error
}

type BatchWriter struct {
	input chan WriterData
	kv    *KV
	lock  sync.Mutex
}

func GetWriteData(k1, k2, k3, k4, k5 string, value []byte, seprator string) WriterData {
	data := k1 + seprator + k2 + seprator + k3 + seprator + k4
	if k5 != "" {
		data = data + seprator + k5
	}
	e := make(chan error, 2)
	return WriterData{[]byte(data), value, true, e}
}

func (b *BatchWriter) Write(value []byte, k ...string) error {
	e := make(chan error, 2)
	w := WriterData{[]byte(strings.Join(k, b.kv.D)), value, true, e}
	b.input <- w
	err := <-w.err
	close(w.err)
	return err
}

func (b *BatchWriter) Delete(key ...string) error {
	e := make(chan error, 2)
	w := WriterData{Key: []byte(strings.Join(key, b.kv.D)), Type: false, err: e}
	b.input <- w
	err := <-w.err
	close(w.err)
	return err
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
		if r != nil {
			Log.Error().Interface("recovered", r).Interface("stack", string(debug.Stack())).Msg("Recovered in BatchWriter.mainRoutine ")
			b.mainRoutine(id, input, internal, db)
		}
	}()
	var transaction *badger.Txn
	var transactionCount int
	for {
		select {
		case data := <-input:
			if transaction == nil {
				transaction = db.NewTransaction(true)
			}
			var err error
			if data.Type {
				err = transaction.Set(data.Key, data.Value)
			} else {
				err = transaction.Delete(data.Key)
			}
			transactionCount++
			if err != nil {
				err = transaction.Commit()
				if err != nil {
					Log.Error().Interface("error", err).Interface("transaction", transaction).Msg("Unable to commit write transaction")
				}
				transaction = db.NewTransaction(true)
				if data.Type {
					err = transaction.Set(data.Key, data.Value)
				} else {
					err = transaction.Delete(data.Key)
				}
				if err != nil {
					Log.Error().Interface("error", err).Str("key", string(data.Key)).Str("value", string(data.Value)).Msg("Unable to set key")
				} else {
					transactionCount = 1
				}
			}
			data.err <- err
		case <-internal:
			if transactionCount > 0 && len(input) == 0 {
				err := transaction.Commit()
				if err != nil {
					Log.Error().Interface("error", err).Interface("transaction", transaction).Msg("Unable to commit write transaction")
				}
				transaction = db.NewTransaction(true)
				transactionCount = 0
			}
		}
	}
}

func (b *BatchWriter) childRoutine(chs []chan bool) {
	defer func() {
		r := recover()
		if r != nil {
			Log.Error().Interface("recovered", r).Interface("stack", string(debug.Stack())).Msg("Recovered in  BatchWriter.childRoutine ")
			b.childRoutine(chs)
		}
	}()
	for {
		time.Sleep(500 * time.Millisecond)
		for _, ch := range chs {
			ch <- true
		}
	}
}

func (b *BatchWriter) Close() {
	for {
		if len(b.input) == 0 {
			break
		}
	}
}
