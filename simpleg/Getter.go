package simpleg

import (
	"errors"
	"fmt"
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
	Objects    []map[string][]byte
	IDs        [][]byte
}

type GetterFactory struct {
	DB                          *DB
	Input                       chan Query
	transactionValidityDuration uint64
}

func (g *GetterFactory) LoadObjects(txn *badger.Txn, node NodeQuery, isIds bool) (*ObjectList, []error) {
	ret := ObjectList{}
	ret.isIds = isIds
	var errs []error
	if node.Direction == "" {
		errs = append(errs, ErrLinkInObjectLoader)
		return nil, errs
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
	ot, ok := g.DB.OT[qData[0].(string)]
	if !ok {
		ret.Errors = append(ret.Errors, errObjectTypeNotFound)
	}
	newObject := g.DB.OT[qData[0].(string)].New(g.DB)
	(*data)[qData[1].(string)] = newObject
}
