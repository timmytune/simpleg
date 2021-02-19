package keyvalue

import (
	"bytes"
	"context"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/pb"
	"github.com/rs/zerolog"
)

var (
	BadgerDefaultOptions   = badger.DefaultOptions
	DefaultIteratorOptions = badger.DefaultIteratorOptions
	WriterInput            chan WriterData
	lock                   = sync.Mutex{}

	Log *zerolog.Logger
)

func BytesToInt(b []byte) (int, error) {
	return strconv.Atoi(string(b))

}

func IntToBytes(i int) []byte {
	return []byte(strconv.Itoa(i))

}

func add(existing, new []byte) []byte {
	e, err := BytesToInt(existing)
	n, err2 := BytesToInt(new)

	if err != nil && err2 != nil {
		return IntToBytes(1)
	}
	if err != nil && err2 == nil {
		return IntToBytes(n)
	}
	if err == nil && err2 != nil {
		return IntToBytes(e)
	}
	if err == nil && err2 == nil {
		return IntToBytes(e + n)
	}
	return IntToBytes(0)
}

type KVOption struct {
	D                              string
	WriteTransactionsChannelLength int
	WriterRoutines                 int
	WriteTransactionsLimit         int
}

// KV is the default key value writer using badger
type KV struct {
	DB                       *badger.DB
	Writer                   *badger.WriteBatch
	Seqs                     map[string]*badger.Sequence
	D                        string
	Writer2                  *BatchWriter
	writeTransactionsChannel chan *badger.Txn
	writeTransactionsCount   int
	writeTransactionsLimit   int
}

// Open is constructor function to create badger instance,
// configure defaults and return struct instance
func GetDefaultKVOptions() KVOption {
	return KVOption{D: "^", WriteTransactionsLimit: 500, WriteTransactionsChannelLength: 50, WriterRoutines: 10}
}

func Open(kvOption KVOption, badgerOption badger.Options) (*KV, error) {
	s := KV{D: kvOption.D}
	db, err := badger.Open(badgerOption)
	if err != nil {
		log.Println("errr", err)
		Log.Fatal().Interface("error", err).Msg("initialization failed")
		return nil, err
	}
	s.DB = db
	s.Writer = db.NewWriteBatch()
	s.writeTransactionsLimit = kvOption.WriteTransactionsLimit
	s.writeTransactionsChannel = make(chan *badger.Txn, kvOption.WriteTransactionsChannelLength)
	go s.setWriteTransaction()
	s.Seqs = make(map[string]*badger.Sequence)
	writer := BatchWriter{}
	WriterInput = writer.Go(kvOption.WriteTransactionsChannelLength, db, &s, kvOption.WriterRoutines)
	s.Writer2 = &writer
	return &s, err
}

func (s *KV) GetWriteTransaction() *badger.Txn {
	tnx := <-s.writeTransactionsChannel
	return tnx
}

func (s *KV) DoneWriteTransaction() {
	lock.Lock()
	defer lock.Unlock()
	s.writeTransactionsCount--
}

func (s *KV) setWriteTransaction() {
	defer func() {
		r := recover()
		if r != nil {
			(*Log).Print("Recovered in set write Transaction from error")
			s.setWriteTransaction()
		}

	}()

	for {
		if s.writeTransactionsCount <= s.writeTransactionsLimit {
			txn := s.DB.NewTransaction(true)
			s.writeTransactionsChannel <- txn
			lock.Lock()
			s.writeTransactionsCount++
			lock.Unlock()
		} else {
			time.Sleep(50 * time.Millisecond)
		}

	}

}

// GetNextID Retrievies the next available interger for the provided key
func (s *KV) GetNextID(key string, limit uint64) (uint64, error) {
	lock.Lock()
	defer lock.Unlock()
	var e error
	var i uint64
	_, ok := s.Seqs[key]
	if !ok {
		s.Seqs[key], e = s.DB.GetSequence([]byte(key), limit)
	}
	if e != nil {
		return i, e
	}

	i, e = s.Seqs[key].Next()
	if i == uint64(0) {
		i, e = s.Seqs[key].Next()
	}
	return i, e

}

func (s *KV) CombineKey(key ...string) []byte {
	return []byte(strings.Join(key, s.D))
}

func (s *KV) Set(one, two, three, four, five string, value []byte) error {
	var err error

	var buffer bytes.Buffer
	buffer.WriteString(one)
	buffer.WriteString(s.D)
	buffer.WriteString(two)
	buffer.WriteString(s.D)
	buffer.WriteString(three)
	buffer.WriteString(s.D)
	buffer.WriteString(four)
	if five != "" {
		buffer.WriteString(s.D)
		buffer.WriteString(five)
	}

	err = s.DB.Update(func(txn *badger.Txn) error {
		err = txn.Set(buffer.Bytes(), value)
		return err
	})
	return err
}

// Set passes a key & value to badger. Expects string for both
// key and value for convenience, unlike badger itself
func (s *KV) Write(one, two, three, four, five string, value []byte) error {
	var err error

	var buffer bytes.Buffer
	buffer.WriteString(one)
	buffer.WriteString(s.D)
	buffer.WriteString(two)
	buffer.WriteString(s.D)
	buffer.WriteString(three)
	buffer.WriteString(s.D)
	buffer.WriteString(four)
	if five != "" {
		buffer.WriteString(s.D)
		buffer.WriteString(five)
	}
	err = s.Writer.Set(buffer.Bytes(), value)

	return err
}

func (s *KV) WriteDelete(key ...string) error {
	var err error
	err = s.Writer.Delete([]byte(strings.Join(key, s.D)))
	return err
}

// Stream a very
func (s *KV) Stream(prefix []string, chooseKey func(*badger.Item) bool) (*pb.KVList, error) {
	var err error
	var w *pb.KVList

	stream := s.DB.NewStream()

	stream.NumGo = 4 // Set number of goroutines to use for iteration.

	stream.LogPrefix = "Badger.Streaming" // For identifying stream logs. Outputs to Logger.

	if chooseKey != nil {
		stream.ChooseKey = chooseKey
	}

	stream.KeyToList = nil

	stream.Send = func(list *pb.KVList) error {
		var err error
		w = list // Write to w.
		return err
	}

	// Run the stream
	if err = stream.Orchestrate(context.Background()); err != nil {
		return nil, err
	}

	return w, err
	// Done.
}

// FlushWrites Persist all pending writes
func (s *KV) FlushWrites() error {
	var err error
	err = s.Writer.Flush()
	return err
}

// Get returns value of queried key from badger
func (s *KV) Get(key ...string) ([]byte, error) {
	var val []byte
	var err error
	err = s.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(strings.Join(key, s.D)))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	return val, err
}

// Read: returns the keys and values that matched the prefix
func (s *KV) Read(key ...string) (map[string][]byte, map[string]error) {
	ret := make(map[string][]byte)
	err := make(map[string]error)

	prefix := []byte(strings.Join(key, s.D))

	s.DB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			er := item.Value(func(v []byte) error {
				ret[string(k)] = v
				return nil
			})
			if er != nil {
				err[string(k)] = er
			}
		}
		return nil
	})

	return ret, err
}

// AddNum: get merge operator for key and increment remember to call .close on ther merge operator
func (s *KV) AddNum(key string) *badger.MergeOperator {
	m := s.DB.GetMergeOperator([]byte(key), add, 200*time.Millisecond)
	return m
}

// Delete removes a key and its value from badger instance
func (s *KV) Delete(key ...string) error {
	var err error
	err = s.DB.Update(func(txn *badger.Txn) error {
		err = txn.Delete([]byte(strings.Join(key, s.D)))
		return err
	})
	return err
}

// DeletePrefix removes a key and its value from badger instance
func (s *KV) DeletePrefix(key ...string) error {
	var err error
	err = s.DB.DropPrefix([]byte(strings.Join(key, s.D)))
	return err
}

// Close wraps badger Close method for defer
func (s *KV) Close() error {
	for _, v := range s.Seqs {
		v.Release()
	}
	s.Writer2.Close()
	return s.DB.Close()
}
