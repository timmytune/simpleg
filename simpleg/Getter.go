package simpleg

import (
	"errors"
	"fmt"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v2"
)

var (
	ErrObjectTypeNotFound = errors.New("Object Type is not saved in the Database")
	ErrLinkInObjectLoader = errors.New("Object Cannot load Link data")
)

type NodeQueryInstruction struct {
	action string
	param  interface{}
}

type NodeQuery struct {
	Direction    string
	TypeName     string
	Instructions map[string][]NodeQueryInstruction
	Skip         int
	Limit        int
	Sort         string
	SortType     bool
	saveName     string
	index        string
}

func (g *NodeQuery) Object(typ string) *NodeQuery {
	g.TypeName = typ
	return g
}
func (g *NodeQuery) Link(typ string, direction string) *NodeQuery {
	g.TypeName = typ
	g.Direction = direction
	return g
}
func (g *NodeQuery) Order(skip int, limit int, sort string, sortType string) *NodeQuery {
	g.Skip = skip
	g.Limit = limit
	g.Sort = sort
	if sortType == "asc" {
		g.SortType = true
	}
	return g
}
func (g *NodeQuery) Name(name string) *NodeQuery {
	g.saveName = name
	return g
}
func (g *NodeQuery) Q(fieldName string, action string, param interface{}) *NodeQuery {
	if g.Instructions == nil {
		g.Instructions = make(map[string][]NodeQueryInstruction)
		g.index = fieldName
	}
	_, ok := g.Instructions[fieldName]
	if !ok {
		g.Instructions[fieldName] = make([]NodeQueryInstruction, 1)
	}
	g.Instructions[fieldName] = append(g.Instructions[fieldName], NodeQueryInstruction{action, param})
	return g
}

type QueryInstruction struct {
	Action string
	Params []interface{}
}

type Query struct {
	Instructions []QueryInstruction
	Ret          chan GetterRet
	DB           *DB
	ReturnType   int
}

func (q *Query) Populate(args ...interface{}) *Query {
	if q.Instructions == nil {
		q.Instructions = make([]QueryInstruction, 1)
	}
	q.Instructions = append(q.Instructions, QueryInstruction{"populate", args})
	return q
}
func (q *Query) Do(action string, args ...interface{}) *Query {
	if q.Instructions == nil {
		q.Instructions = make([]QueryInstruction, 1)
	}
	q.Instructions = append(q.Instructions, QueryInstruction{action, args})
	return q
}
func (q *Query) Return(returnType string, args ...interface{}) GetterRet {
	q.Ret = make(chan GetterRet)
	q.Instructions = append(q.Instructions, QueryInstruction{"return", args})
	switch returnType {
	case "array":
		q.ReturnType = 1
	case "map":
		q.ReturnType = 2
	}
	q.DB.Getter.Input <- *q
	ret := <-q.Ret
	return ret
}

type GetterRet struct {
	Data   interface{}
	Errors []error
}

type ObjectList struct {
	ObjectName string
	isIds      bool
	Objects    []map[KeyValueKey][]byte
	IDs        [][]byte
}

type GetterFactory struct {
	DB                          *DB
	Input                       chan Query
	transactionValidityDuration uint64
}
type KeyValueKey struct {
	Main string
	Subs string
}

func (k *KeyValueKey) Set(b []byte, d string, index int) error {
	st := string(b)
	sts := strings.Split(st, d)
	l := len(sts)
	if l > index {
		k.Main = sts[index]
	} else {
		return errors.New("Main index not available in key" + st)
	}
	if l > (index + 1) {
		k.Subs = strings.Join(sts[index+1:], d)
	}
	return nil

}

func (g *GetterFactory) getKeysWithValue(txn *badger.Txn, pre ...string) (map[KeyValueKey][]byte, []error) {
	ret := make(map[KeyValueKey][]byte)
	errs := make([]error, 0)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	pr := []byte(strings.Join(pre, g.DB.KV.D))
	for it.Seek(pr); it.ValidForPrefix(pr); it.Next() {
		item := it.Item()
		k := item.Key()
		v, err := item.ValueCopy(nil)
		if err != nil {
			errs = append(errs, err)
		} else {
			key := KeyValueKey{}
			err := key.Set(k, g.DB.KV.D, 3)
			if err != nil {
				errs = append(errs, err)
			} else {
				ret[key] = v
			}

		}
	}
	return ret, errs
}

func (g *GetterFactory) LoadObjects(txn *badger.Txn, node NodeQuery, isIds bool) (*ObjectList, []error) {
	ret := ObjectList{}
	ret.isIds = isIds
	var errs []error
	if node.Direction == "" {
		errs = append(errs, ErrLinkInObjectLoader)
		return nil, errs
	}
	ins, ok := node.Instructions["ID"]
	if ok {
		for _, query := range ins {
			ret.ObjectName = node.TypeName
			if query.action == "==" {
				if isIds {
					// just return an object list with the id requested
					ret.IDs = make([][]byte, 1)
					g.DB.Lock.Lock()
					ret.IDs = append(ret.IDs, g.DB.FT["uint64"].Set(query.param))
					g.DB.Lock.Unlock()
					return &ret, errs
				} else {
					g.DB.Lock.Lock()
					rawID := g.DB.FT["uint64"].Set(query.param)
					g.DB.Lock.Unlock()
					obj, err := g.getKeysWithValue(txn, g.DB.Options.DBName, node.TypeName, string(rawID))
					if len(err) > 0 {
						errs = append(errs, err...)
					}
					if obj != nil {
						ret.Objects = append(ret.Objects, obj)
					}

				}
				return &ret, errs
			}
		}
	}

	return &ret, errs
}
func (g *GetterFactory) Start(db *DB, numOfRuners int, inputChannelLength int, transactionValidityDuration uint64) {
	g.DB = db
	g.transactionValidityDuration = transactionValidityDuration
	g.Input = make(chan Query, inputChannelLength)
	for i := 0; i < numOfRuners; i++ {
		go g.Run()
	}
}
func (g *GetterFactory) Run() {
	var job Query
	var ret GetterRet
	var data map[string]interface{}
	var txn *badger.Txn
	defer func() {
		r := recover()
		if r != nil {
			fmt.Println("Recovered in GetterFactory.Run ", r)

			if ret.Errors == nil {
				ret.Errors = make([]error, 1)
			}
			switch x := r.(type) {
			case string:
				ret.Errors = append(ret.Errors, errors.New(x))
			case error:
				ret.Errors = append(ret.Errors, x)
			default:
				ret.Errors = append(ret.Errors, errors.New("Unknown error was thrown"))
			}
			if job.Ret != nil {
				job.Ret <- ret
				close(job.Ret)
			}
			if txn != nil {
				txn.Discard()
				txn = nil
			}
			g.Run()
		}
	}()

	for {
		job = <-g.Input
		ret = GetterRet{}
		ret.Errors = make([]error, 1)
		data = make(map[string]interface{})
		if txn == nil {
			txn = g.DB.KV.DB.NewTransaction(false)
		}
		if uint64(time.Now().Unix()) > (txn.ReadTs() + g.transactionValidityDuration) {
			txn.Discard()
			txn = g.DB.KV.DB.NewTransaction(false)
		}
		for _, val := range job.Instructions {
			switch val.Action {
			case "return":
				GetterReturn(g, txn, &data, &job, val.Params, &ret)
			case "new.object":
				GetterNewObject(g, txn, &data, &job, val.Params, &ret)

			}

		}

	}

}

//GetterNewObject ..
//action: 'new.object'
//params [0] Object name (String)
//return New object
func GetterNewObject(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	g.DB.Lock.Lock()
	ot, ok := g.DB.OT[qData[0].(string)]
	g.DB.Lock.Unlock()
	if !ok {
		ret.Errors = append(ret.Errors, ErrObjectTypeNotFound)
	}
	ret.Data = ot.New(g.DB)
	q.Ret <- *ret
}

//GetterReturn ..
//action: 'return'
//params [0] Object name (String)
//return interface{}
func GetterReturn(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	if q.ReturnType == 0 {

	}
	g.DB.Lock.Lock()
	ot, ok := g.DB.OT[qData[0].(string)]
	g.DB.Lock.Unlock()
	if !ok {
		ret.Errors = append(ret.Errors, ErrObjectTypeNotFound)
	}
	newObject := g.DB.OT[qData[0].(string)].New(g.DB)
	(*data)[qData[1].(string)] = newObject
}
