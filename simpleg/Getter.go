package simpleg

import (
	"bytes"
	"errors"
	"runtime/debug"
	"sort"
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
	skip         int
	limit        int
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
func (g *NodeQuery) Order(sort string, sortType string) *NodeQuery {
	g.Sort = sort
	if sortType == "asc" {
		g.SortType = true
	}
	return g
}
func (g *NodeQuery) Limit(limit int) *NodeQuery {
	g.limit = limit
	return g
}
func (g *NodeQuery) Skip(skip int) *NodeQuery {
	g.skip = skip
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
		g.Instructions[fieldName] = make([]NodeQueryInstruction, 0)
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
		q.Instructions = make([]QueryInstruction, 0)
	}
	q.Instructions = append(q.Instructions, QueryInstruction{"populate", args})
	return q
}
func (q *Query) Do(action string, args ...interface{}) *Query {
	if q.Instructions == nil {
		q.Instructions = make([]QueryInstruction, 0)
	}
	q.Instructions = append(q.Instructions, QueryInstruction{Action: action, Params: args})
	return q
}
func (q *Query) Return(returnType string, args ...interface{}) GetterRet {
	q.Ret = make(chan GetterRet)
	if returnType != "skip" {
		q.Instructions = append(q.Instructions, QueryInstruction{"return", args})
		switch returnType {
		case "single":
			q.ReturnType = 1
		case "array":
			q.ReturnType = 2
		case "map":
			q.ReturnType = 3
		}
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
	order      struct {
		field string
		typ   bool
	}
}

type LinkList struct {
	LinkName string
	isIds    bool
	Links    []map[KeyValueKey][]byte
	IDs      []struct {
		FROM []byte
		TO   []byte
	}
	order struct {
		field string
		typ   bool
	}
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
func (k *KeyValueKey) GetFullString(d string) string {
	if k.Subs == "" {
		return k.Main
	}
	return k.Main + d + k.Subs
}

type iteratorLoader struct {
	txn       *badger.Txn
	notFirst  bool
	prefix    string
	fieldType FieldType
	query     []NodeQueryInstruction
	db        *DB
	iterator  *badger.Iterator
	field     string
	obj       string
	indexed   bool
	reverse   bool
	left      struct {
		ins string
		val string
	}
	center struct {
		ins string
		val string
	}
	right struct {
		ins string
		val string
	}
}

func (i *iteratorLoader) setup(db *DB, obj string, field string, inst []NodeQueryInstruction, txn *badger.Txn) []error {
	var errs []error
	db.RLock()
	vaa, ok := db.OT[obj].Fields[field]
	if !ok {
		errs = append(errs, errors.New("Field -"+field+"- Not found for object -"+obj+"- in the Database"))
	}
	i.indexed = vaa.Indexed
	i.fieldType = db.FT[db.OT[obj].Fields[field].FieldType]
	i.field = field
	i.txn = txn
	i.obj = obj
	db.RUnlock()
	i.db = db
	index := ""
	//i.iterator = txn.NewIterator()

	if i.indexed {
		for _, d := range inst {
			val, ins, err := i.fieldType.CompareIndexed(d.action, d.param)
			if err != nil {
				errs = append(errs, err)
			}
			switch ins {
			case "==":
				i.center.ins = ins
				i.center.val = val
			case ">=":
				i.left.ins = ins
				i.left.val = val
			case ">":
				i.left.ins = ins
				i.left.val = val
			case "<=":
				i.right.ins = ins
				i.right.val = val
			case "<":
				i.right.ins = ins
				i.right.val = val
			}
		}
		if i.center.ins != "" {
			index = i.center.val
		} else if i.left.ins != "" && i.right.ins == "" {
			index = i.left.val
		} else if i.left.ins != "" && i.right.ins != "" {
			index = i.left.val
		} else if i.left.ins == "" && i.right.ins != "" {
			index = i.right.val
			i.reverse = true
		}
		i.prefix = db.Options.DBName + db.KV.D + obj + db.KV.D + field + db.KV.D + index
		opt := badger.DefaultIteratorOptions
		opt.Prefix = []byte(i.prefix)
		opt.PrefetchSize = 20
		opt.Reverse = i.reverse
		opt.PrefetchValues = false
		i.iterator = txn.NewIterator(opt)
	} else {
		i.prefix = db.Options.DBName + db.KV.D + obj + db.KV.D + "ID"
		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false
		opt.Prefix = []byte(i.prefix)
		opt.PrefetchSize = 20
		i.iterator = txn.NewIterator(opt)
		i.query = inst
	}
	i.iterator.Seek([]byte(i.prefix))
	return errs
}
func (i *iteratorLoader) close() {
	if i.iterator != nil {
		i.iterator.Close()
	}
}
func (i *iteratorLoader) next() (map[KeyValueKey][]byte, bool, error) {
	r := make(map[KeyValueKey][]byte)
	var k []byte
	var kArray [][]byte
	notValid := true

	if i.notFirst {
		i.iterator.Next()
	} else {
		i.notFirst = true
	}
	for notValid {
		item := i.iterator.Item()
		k = item.KeyCopy(k)
		kArray = bytes.Split(k, []byte(i.db.KV.D))
		if i.indexed {
			// check if the key is for this field, if not go to the next one and check again, if test faild 2 times return
			if string(kArray[2]) != i.field {
				i.iterator.Next()
				item = i.iterator.Item()
				k = item.KeyCopy(k)
				kArray = bytes.Split(k, []byte(i.db.KV.D))
				if string(kArray[2]) != i.field {
					return r, false, nil
				}

			}
			passed := 0
			if i.center.ins != "" {
				if i.iterator.ValidForPrefix([]byte(i.prefix)) {
					passed++
				} else {
					return r, false, nil
				}
			} else {
				passed++
			}

			if i.left.ins != "" {
				d := bytes.Compare(kArray[3], []byte(i.left.val))
				if d == -1 && i.reverse {
					return r, false, nil
				}
				if i.left.ins == ">" && d == 1 {
					passed++
				}
				if i.left.ins == ">=" && (d == 1 || d == 0) {
					passed++
				}
			} else {
				passed++
			}

			if i.right.ins != "" {
				d := bytes.Compare(kArray[3], []byte(i.right.val))
				if d == 1 && !i.reverse {
					return r, false, nil
				}
				if i.right.ins == "<" && d == -1 {
					passed++
				}
				if i.right.ins == "<=" && (d == -1 || d == 0) {
					passed++
				}
			} else {
				passed++
			}

			if passed > 2 {
				notValid = false
				r[KeyValueKey{Main: string(kArray[2])}] = kArray[3]
				r[KeyValueKey{Main: "ID"}] = kArray[4]
			} else {
				i.iterator.Next()
			}

		} else {
			if !i.iterator.ValidForPrefix([]byte(i.prefix)) {
				return r, false, nil
			}
			var v []byte
			var buffer bytes.Buffer
			buffer.WriteString(i.db.Options.DBName)
			buffer.WriteString(i.db.KV.D)
			buffer.WriteString(i.obj)
			buffer.WriteString(i.db.KV.D)
			buffer.Write(kArray[3])
			buffer.WriteString(i.db.KV.D)
			buffer.WriteString(i.field)
			item2, err := i.txn.Get(buffer.Bytes())
			if err != nil && err != badger.ErrKeyNotFound {
				Log.Error().Interface("error", err).Interface("stack", debug.Stack()).Str("key", string(k)).Msg("Getting value for key in Badger threw error")
				return nil, false, err
			}
			v, err = item2.ValueCopy(v)
			if err != nil {
				Log.Error().Interface("error", err).Interface("stack", debug.Stack()).Str("key", string(buffer.Bytes())).Msg("Getting value for key in Badger threw error")
				return nil, false, err
			}

			boa := false
			for _, ins := range i.query {
				rawQueryData, err := i.fieldType.Set(ins.param)
				if err != nil {
					return nil, false, err
				}
				boa, err = i.fieldType.Compare(ins.action, v, rawQueryData)
				if err != nil {
					return nil, false, err
				}
				if !boa {
					break
				}
			}
			if boa {
				notValid = false
				others := ""
				if len(kArray) > 4 {
					ks := string(item2.KeyCopy(nil))
					ksa := strings.Split(ks, i.db.KV.D)
					others = strings.Join(ksa[4:], i.db.KV.D)
				}
				r[KeyValueKey{Main: i.field, Subs: others}] = v
				r[KeyValueKey{Main: "ID"}] = kArray[3]
			} else {
				i.iterator.Next()
			}

		}
	}

	return r, true, nil
}

type GetterFactory struct {
	DB                          *DB
	Input                       chan Query
	transactionValidityDuration uint64
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
func (g *GetterFactory) getObjectArray(o *ObjectList) ([]interface{}, []error) {
	var errs []error
	if o.order.field != "" && !o.isIds {
		sort.Slice(o.Objects, func(i, j int) bool {
			b := bytes.Compare(o.Objects[i][KeyValueKey{Main: o.order.field}], o.Objects[j][KeyValueKey{Main: o.order.field}])
			if b == 1 || b == 0 {
				if o.order.typ {
					return false
				} else {
					return true
				}
			} else {
				if o.order.typ {
					return true
				} else {
					return false
				}
			}
		})
	}
	if o.isIds {
		errs = append(errs, errors.New("Can't get objects for type "+o.ObjectName))
		return nil, errs
	}
	ret := make([]interface{}, 0)
	g.DB.RLock()
	ot, ok := g.DB.OT[o.ObjectName]
	g.DB.RUnlock()
	if !ok {
		errs = append(errs, errors.New("Can't find object of name "+o.ObjectName))
		return nil, errs
	}
	for _, val := range o.Objects {
		v, errs2 := ot.Get(val, g.DB)
		if len(errs2) > 0 {
			errs = append(errs, errs2...)
		} else {
			ret = append(ret, v)
		}

	}
	return ret, errs
}
func (g *GetterFactory) LoadObjects(txn *badger.Txn, node NodeQuery, isIds bool) (*ObjectList, []error) {
	ret := ObjectList{}
	ret.isIds = isIds
	ret.ObjectName = node.TypeName
	ret.order.field = node.Sort
	ret.order.typ = node.SortType
	objectSkips := 0
	objectCount := 0
	var errs []error
	if node.limit == 0 {
		node.limit = 100
	}
	if isIds {
		ret.IDs = make([][]byte, 0)
	} else {
		ret.Objects = make([]map[KeyValueKey][]byte, 0)
	}
	if node.Direction != "" {
		errs = append(errs, ErrLinkInObjectLoader)
		return nil, errs
	}
	ins, ok := node.Instructions["ID"]
	if ok {
		for _, query := range ins {
			if query.action == "==" {
				if isIds {
					// just return an object list with the id requested
					ret.IDs = make([][]byte, 0)
					g.DB.RLock()
					id, err := g.DB.FT["uint64"].Set(query.param)
					if err != nil {
						errs = append(errs, err)
					} else {
						ret.IDs = append(ret.IDs, id)
					}

					g.DB.RUnlock()
					return &ret, errs
				} else {
					g.DB.RLock()
					rawID, er := g.DB.FT["uint64"].Set(query.param)
					g.DB.RUnlock()
					if er != nil {
						errs = append(errs, er)
					} else {
						obj, err := g.getKeysWithValue(txn, g.DB.Options.DBName, node.TypeName, string(rawID))
						if len(err) > 0 {
							errs = append(errs, err...)
						}
						if obj != nil {
							obj[KeyValueKey{Main: "ID"}] = rawID
							ret.Objects = append(ret.Objects, obj)
						}
					}

				}
				return &ret, errs
			}

		}
	}

	iterator := iteratorLoader{}
	errs = iterator.setup(g.DB, node.TypeName, node.index, node.Instructions[node.index], txn)
	defer iterator.close()

	for d, b, e := iterator.next(); b; d, b, e = iterator.next() {
		failed := false
		if e != nil {
			errs = append(errs, e)
		}
		if d != nil {
			delete(node.Instructions, node.index)
			for key, in1 := range node.Instructions {
				g.DB.RLock()
				var fieldType FieldType
				va, ok := g.DB.OT[node.TypeName].Fields[key]
				if !ok {
					errs = append(errs, errors.New("Field name -"+key+"- not found in database"))
					g.DB.RUnlock()
					return &ret, errs
				} else {
					fieldType = g.DB.FT[va.FieldType]
				}
				g.DB.RUnlock()

				var buffer bytes.Buffer
				buffer.WriteString(g.DB.Options.DBName)
				buffer.WriteString(g.DB.KV.D)
				buffer.WriteString(node.TypeName)
				buffer.WriteString(g.DB.KV.D)
				buffer.Write(d[KeyValueKey{Main: "ID"}])
				buffer.WriteString(g.DB.KV.D)
				buffer.WriteString(key)
				item, err := txn.Get(buffer.Bytes())
				if err != nil && err == badger.ErrKeyNotFound {
				} else if err != nil && err != badger.ErrKeyNotFound {
					Log.Error().Interface("error", err).Interface("stack", debug.Stack()).Msg("Getting value for key in Badger threw error again")
					errs = append(errs, err)
					return &ret, errs
				} else if err == nil && item != nil {
					var val []byte
					for _, in2 := range in1 {
						rawParam, err := fieldType.Set(in2.param)
						if err != nil {
							errs = append(errs, err)
							return &ret, errs
						}
						val, err = item.ValueCopy(nil)
						if err != nil {
							Log.Error().Interface("error", err).Str("key", string(item.KeyCopy(nil))).Interface("stack", debug.Stack()).Msg("Getting value for key after getting item in Badger threw error")
							errs = append(errs, err)
							return &ret, errs
						}
						ok, err := fieldType.Compare(in2.action, val, rawParam)
						if err != nil {
							errs = append(errs, err)
							return &ret, errs
						}
						if !ok {
							failed = true
						}
					}
					if !failed {
						if !isIds {
							d[KeyValueKey{Main: key}] = val
						}
					}
				}

			}
			if !failed {
				if node.skip > objectSkips {
					objectSkips++
				} else {
					if node.limit <= objectCount {
						break
					}
					if isIds {
						ret.IDs = append(ret.IDs, d[KeyValueKey{Main: "ID"}])
					} else {
						notIncluded := make([]string, 0)
						g.DB.RLock()
						for k := range g.DB.OT[node.TypeName].Fields {
							_, ok = d[KeyValueKey{Main: k}]
							if !ok {
								notIncluded = append(notIncluded, k)
							}
						}
						g.DB.RUnlock()
						for _, v := range notIncluded {
							var buffer bytes.Buffer
							buffer.WriteString(g.DB.Options.DBName)
							buffer.WriteString(g.DB.KV.D)
							buffer.WriteString(node.TypeName)
							buffer.WriteString(g.DB.KV.D)
							buffer.Write(d[KeyValueKey{Main: "ID"}])
							buffer.WriteString(g.DB.KV.D)
							buffer.WriteString(v)

							item, err := txn.Get(buffer.Bytes())
							if err != nil && err != badger.ErrKeyNotFound {
								errs = append(errs, err)
							}
							if item != nil {
								val, err := item.ValueCopy(nil)
								if err != nil {
									errs = append(errs, err)
								} else {
									d[KeyValueKey{Main: v}] = val
								}
							}
						}
						ret.Objects = append(ret.Objects, d)
					}
					objectCount++
				}

			}
		}
	}
	return &ret, errs
}
func (g *GetterFactory) LoadLinks(txn *badger.Txn, node NodeQuery, isIds bool) (*LinkList, []error) {
	ret := LinkList{}
	ret.isIds = isIds
	ret.LinkName = node.TypeName
	ret.order.field = node.Sort
	ret.order.typ = node.SortType
	linkSkips := 0
	linkCount := 0
	var errs []error
	if node.limit == 0 {
		node.limit = 100
	}
	if isIds {
		ret.IDs = make([][]byte, 0)
	} else {
		ret.Objects = make([]map[KeyValueKey][]byte, 0)
	}
	if node.Direction != "" {
		errs = append(errs, ErrLinkInObjectLoader)
		return nil, errs
	}
	ins, ok := node.Instructions["FROM"]
	ins2, ok2 := node.Instructions["TO"]
	if ok {
		for _, query := range ins {
			if query.action == "==" {
				if isIds {
					// just return an object list with the id requested
					ret.IDs = make([][]byte, 0)
					g.DB.RLock()
					id, err := g.DB.FT["uint64"].Set(query.param)
					if err != nil {
						errs = append(errs, err)
					} else {
						ret.IDs = append(ret.IDs, id)
					}

					g.DB.RUnlock()
					return &ret, errs
				} else {
					g.DB.RLock()
					rawID, er := g.DB.FT["uint64"].Set(query.param)
					g.DB.RUnlock()
					if er != nil {
						errs = append(errs, er)
					} else {
						obj, err := g.getKeysWithValue(txn, g.DB.Options.DBName, node.TypeName, string(rawID))
						if len(err) > 0 {
							errs = append(errs, err...)
						}
						if obj != nil {
							obj[KeyValueKey{Main: "ID"}] = rawID
							ret.Objects = append(ret.Objects, obj)
						}
					}

				}
				return &ret, errs
			}

		}
	}

	iterator := iteratorLoader{}
	errs = iterator.setup(g.DB, node.TypeName, node.index, node.Instructions[node.index], txn)
	defer iterator.close()

	for d, b, e := iterator.next(); b; d, b, e = iterator.next() {
		failed := false
		if e != nil {
			errs = append(errs, e)
		}
		if d != nil {
			delete(node.Instructions, node.index)
			for key, in1 := range node.Instructions {
				g.DB.RLock()
				var fieldType FieldType
				va, ok := g.DB.OT[node.TypeName].Fields[key]
				if !ok {
					errs = append(errs, errors.New("Field name -"+key+"- not found in database"))
					g.DB.RUnlock()
					return &ret, errs
				} else {
					fieldType = g.DB.FT[va.FieldType]
				}
				g.DB.RUnlock()

				var buffer bytes.Buffer
				buffer.WriteString(g.DB.Options.DBName)
				buffer.WriteString(g.DB.KV.D)
				buffer.WriteString(node.TypeName)
				buffer.WriteString(g.DB.KV.D)
				buffer.Write(d[KeyValueKey{Main: "ID"}])
				buffer.WriteString(g.DB.KV.D)
				buffer.WriteString(key)
				item, err := txn.Get(buffer.Bytes())
				if err != nil && err == badger.ErrKeyNotFound {
				} else if err != nil && err != badger.ErrKeyNotFound {
					Log.Error().Interface("error", err).Interface("stack", debug.Stack()).Msg("Getting value for key in Badger threw error again")
					errs = append(errs, err)
					return &ret, errs
				} else if err == nil && item != nil {
					var val []byte
					for _, in2 := range in1 {
						rawParam, err := fieldType.Set(in2.param)
						if err != nil {
							errs = append(errs, err)
							return &ret, errs
						}
						val, err = item.ValueCopy(nil)
						if err != nil {
							Log.Error().Interface("error", err).Str("key", string(item.KeyCopy(nil))).Interface("stack", debug.Stack()).Msg("Getting value for key after getting item in Badger threw error")
							errs = append(errs, err)
							return &ret, errs
						}
						ok, err := fieldType.Compare(in2.action, val, rawParam)
						if err != nil {
							errs = append(errs, err)
							return &ret, errs
						}
						if !ok {
							failed = true
						}
					}
					if !failed {
						if !isIds {
							d[KeyValueKey{Main: key}] = val
						}
					}
				}

			}
			if !failed {
				if node.skip > objectSkips {
					objectSkips++
				} else {
					if node.limit <= objectCount {
						break
					}
					if isIds {
						ret.IDs = append(ret.IDs, d[KeyValueKey{Main: "ID"}])
					} else {
						notIncluded := make([]string, 0)
						g.DB.RLock()
						for k := range g.DB.OT[node.TypeName].Fields {
							_, ok = d[KeyValueKey{Main: k}]
							if !ok {
								notIncluded = append(notIncluded, k)
							}
						}
						g.DB.RUnlock()
						for _, v := range notIncluded {
							var buffer bytes.Buffer
							buffer.WriteString(g.DB.Options.DBName)
							buffer.WriteString(g.DB.KV.D)
							buffer.WriteString(node.TypeName)
							buffer.WriteString(g.DB.KV.D)
							buffer.Write(d[KeyValueKey{Main: "ID"}])
							buffer.WriteString(g.DB.KV.D)
							buffer.WriteString(v)

							item, err := txn.Get(buffer.Bytes())
							if err != nil && err != badger.ErrKeyNotFound {
								errs = append(errs, err)
							}
							if item != nil {
								val, err := item.ValueCopy(nil)
								if err != nil {
									errs = append(errs, err)
								} else {
									d[KeyValueKey{Main: v}] = val
								}
							}
						}
						ret.Objects = append(ret.Objects, d)
					}
					objectCount++
				}

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
			Log.Error().Interface("recovered", r).Interface("stack", string(debug.Stack())).Msg("Recovered in Getter.Run ")
			if ret.Errors == nil {
				ret.Errors = make([]error, 0)
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
		ret.Errors = make([]error, 0)
		data = make(map[string]interface{})
		if txn == nil {
			txn = g.DB.KV.DB.NewTransaction(false)
		}
		if uint64(time.Now().Unix()) > (txn.ReadTs() + g.transactionValidityDuration) {
			txn.Discard()
			txn = g.DB.KV.DB.NewTransaction(false)
		}
		//log.Print("-------------------------------------------- Instruction", job)
		for _, val := range job.Instructions {
			switch val.Action {
			case "return":
				GetterReturn(g, txn, &data, &job, val.Params, &ret)
			case "object.new":
				GetterNewObject(g, txn, &data, &job, val.Params, &ret)
			case "object":
				GetterObjects(g, txn, &data, &job, val.Params, &ret)
			case "link.new":
				GetterNewLink(g, txn, &data, &job, val.Params, &ret)
			default:
				ret.Errors = append(ret.Errors, errors.New("Invalid Istruction in GetterFactory: "+val.Action))
			}

		}

	}

}

//GetterNewObject ..
//action: 'object.new'
//params [0] Object name (String)
//return New object
func GetterNewObject(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	g.DB.RLock()
	ot, ok := g.DB.OT[qData[0].(string)]
	g.DB.RUnlock()
	if !ok {
		ret.Errors = append(ret.Errors, ErrObjectTypeNotFound)
	}
	ret.Data = ot.New(g.DB)
	q.Ret <- *ret
}

//GetterObjects ..
//action: 'objects'
//params [0] NodeQuery (NodeQuery)
//params [1] Object name (String)
//placesses objectLists found in node query [0] in variable [1]
func GetterObjects(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	n := qData[0].(NodeQuery)
	obs, errs := g.LoadObjects(txn, n, false)
	if len(errs) > 0 {
		ret.Errors = append(ret.Errors, errs...)
	}
	(*data)[n.saveName] = obs
}

//GetterReturn ..
//action: 'return'
// for returntype 1. params [0] Object name (String), params [1] Index of the objects to return (int)
//return interface{}
func GetterReturn(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	if q.ReturnType == 1 {
		da := (*data)[qData[0].(string)]
		switch da.(type) {
		case *ObjectList:
			r, errs := g.getObjectArray(da.(*ObjectList))
			if len(errs) > 0 {
				ret.Errors = append(ret.Errors, errs...)
			}
			if r != nil && len(r) > 0 {
				ret.Data = r[qData[1].(int)]
			}
		default:
			ret.Data = (*data)[qData[0].(string)]

		}
	}
	if q.ReturnType == 2 {
		da := (*data)[qData[0].(string)]
		switch da.(type) {
		case *ObjectList:
			r, errs := g.getObjectArray(da.(*ObjectList))
			if len(errs) > 0 {
				ret.Errors = append(ret.Errors, errs...)
			}
			if r != nil && len(r) > 0 {
				ret.Data = r
			}
		}
	}
	if q.ReturnType == 3 {
		da := (*data)[qData[0].(string)]
		switch da.(type) {
		case *ObjectList:
			r, errs := g.getObjectArray(da.(*ObjectList))
			if len(errs) > 0 {
				ret.Errors = append(ret.Errors, errs...)
			}
			if r != nil && len(r) > 0 {
				ret.Data = r
			}
		}
	}
	q.Ret <- *ret
}

//GetterNewLink ..
//action: 'link.new'
//params [0] Link name (String)
//return New link
func GetterNewLink(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	g.DB.RLock()
	lt, ok := g.DB.LT[qData[0].(string)]
	g.DB.RUnlock()
	if !ok {
		ret.Errors = append(ret.Errors, ErrObjectTypeNotFound)
	}
	ret.Data = lt.New(g.DB)
	q.Ret <- *ret
}

//GetterObjects ..
//action: 'objects'
//params [0] NodeQuery (NodeQuery)
//params [1] Object name (String)
//placesses objectLists found in node query [0] in variable [1]
func GetterLinks(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	n := qData[0].(NodeQuery)
	obs, errs := g.LoadObjects(txn, n, false)
	if len(errs) > 0 {
		ret.Errors = append(ret.Errors, errs...)
	}
	(*data)[n.saveName] = obs
}
