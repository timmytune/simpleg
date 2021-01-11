package keyvalue

import (
	"sync"
)

type BatchReader struct {
	input    chan WriterData
	shotdown bool
	kv       *KV
	lock     sync.Mutex
}
