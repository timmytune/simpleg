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
	"bytes"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"

	badger "github.com/dgraph-io/badger/v3"
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
	Fields       string
}

func (g *NodeQuery) Object(typ string) *NodeQuery {
	if g.limit == 0 {
		g.limit = 100
	}
	if g.Instructions == nil {
		g.Instructions = make(map[string][]NodeQueryInstruction)
	}
	g.TypeName = typ
	return g
}
func (g *NodeQuery) Link(typ string, direction string) *NodeQuery {
	if g.limit == 0 {
		g.limit = 100
	}
	if g.Instructions == nil {
		g.Instructions = make(map[string][]NodeQueryInstruction)
	}
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
func (g *NodeQuery) SetFields(f string) *NodeQuery {
	g.Fields = f
	return g
}
func (g *NodeQuery) Q(fieldName string, action string, param interface{}) *NodeQuery {
	if g.Instructions == nil {
		g.Instructions = make(map[string][]NodeQueryInstruction)
	}

	if g.index == "" {
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
func (q *Query) Return(returnType string, args ...interface{}) (g GetterRet) {
	if q.DB.Shotdown {
		g.Errors = append(g.Errors, errors.New("database shoting down"))
		return
	}
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
		default:
			ret := GetterRet{}
			ret.Errors = append(ret.Errors, errors.New("invalid return type provided you can only use one of 'single', 'array' or 'map' "))
			close(q.Ret)
			return ret
		}
	}
	q.DB.Getter.Input <- *q
	g = <-q.Ret
	return g
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
	fields string
}
type LinkListList struct {
	FROM []byte
	TO   []byte
}
type LinkList struct {
	LinkName string
	isIds    bool
	Links    []map[KeyValueKey][]byte
	IDs      []LinkListList
	order    struct {
		field string
		typ   bool
	}
	fields string
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
	prefix2   string
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
	if field == "" {
		field = "ID"
		i.indexed = true
		i.fieldType = db.FT["uint64"]
	} else {
		vaa, ok := db.OT[obj].Fields[field]
		if !ok {
			errs = append(errs, errors.New("Field -"+field+"- Not found for object -"+obj+"- in the Database"))
		}
		i.indexed = vaa.Indexed
		i.fieldType = db.FT[db.OT[obj].Fields[field].FieldType]
	}

	i.field = field
	i.txn = txn
	i.obj = obj
	db.RUnlock()
	i.db = db
	index := ""
	//i.iterator = txn.NewIterator()

	if i.indexed && len(inst) > 0 {
		val, ins, err := i.fieldType.CompareIndexed(inst[0].action, inst[0].param)
		if err != nil {
			errs = append(errs, err)
		}
		index = val
		switch ins {
		case "==":
			i.center.ins = ins
			i.center.val = val
		case "<=":
			i.left.ins = ins
			i.left.val = val
			i.reverse = true
		case "<":
			i.left.ins = ins
			i.left.val = val
			i.reverse = true
		case ">=":
			i.right.ins = ins
			i.right.val = val
		case ">":
			i.right.ins = ins
			i.right.val = val
		case "prefix":
			i.center.ins = ins
			i.center.val = val
		}
		i.query = inst[1:]

		i.prefix = db.Options.DBName + db.KV.D + obj + db.KV.D + field + db.KV.D + index
		i.prefix2 = db.Options.DBName + db.KV.D + obj + db.KV.D + field
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
		opt.Reverse = i.reverse
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
func getItem(i *badger.Iterator) (item *badger.Item, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("badger could not find item")
		}
	}()
	item = i.Item()
	return
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

		item, err := getItem(i.iterator)
		if err != nil {
			return r, false, nil
		}
		k = item.KeyCopy(k)
		kArray = bytes.Split(k, []byte(i.db.KV.D))

		if i.indexed {

			// check if the key is for this field, if not go to the next one and check again, if test faild 2 times return
			if string(kArray[2]) != i.field || string(kArray[1]) != i.obj {
				i.iterator.Next()
				item, err = getItem(i.iterator)
				if err != nil {
					return r, false, nil
				}
				k = item.KeyCopy(k)
				kArray = bytes.Split(k, []byte(i.db.KV.D))
				if string(kArray[2]) != i.field || string(kArray[1]) != i.obj {
					return r, false, nil
				}

			}

			failed := false
			//check indexed indexed instruction
			if i.center.ins != "" {
				isValid := false
				if i.center.ins == "==" {
					isValid = bytes.Equal(kArray[3], []byte(i.center.val))
				} else if i.center.ins == "prefix" {
					isValid = strings.HasPrefix(string(kArray[3]), i.center.val)
				}
				if !isValid {
					failed = true
				}
			} else if i.left.ins != "" {
				d := bytes.Compare(kArray[3], []byte(i.left.val))
				if i.left.ins == "<" && d != -1 {
					failed = true
				}
				if i.left.ins == "<=" && d > 0 {
					failed = true
				}
			} else if i.right.ins != "" {
				d := bytes.Compare(kArray[3], []byte(i.left.val))
				if i.right.ins == ">" && d != 1 {
					failed = true
				}
				if i.right.ins == ">=" && d < 0 {
					failed = true
				}
			}

			//check other none indexed instructions
			for _, ins := range i.query {
				rawQueryData, err := i.fieldType.Set(ins.param)
				if err != nil {
					return nil, false, err
				}
				boa, err := i.fieldType.Compare(ins.action, kArray[3], rawQueryData)
				if err != nil {
					return nil, false, err
				}
				if !boa {
					failed = true
				}
			}

			if !failed {
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

			//var v []byte
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
				Log.Error().Interface("error", err).Interface("stack", string(debug.Stack())).Str("key", string(k)).Msg("Getting value for key in Badger threw error")
				return nil, false, err
			}
			if item2 != nil {
				v, err := item2.ValueCopy(nil)
				if err != nil {
					Log.Error().Interface("error", err).Interface("stack", string(debug.Stack())).Str("key", buffer.String()).Msg("Getting value for key in Badger threw error")
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
			} else {
				i.iterator.Next()
			}

		}
	}

	return r, true, nil
}

type GetterFactory struct {
	DB    *DB
	Input chan Query
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
	if len(ret) == 0 {
		errs = append(errs, errors.New("key returned no result"))
		return ret, errs
	}
	return ret, errs
}
func (g *GetterFactory) getKeysWithValueLinks(txn *badger.Txn, pre ...string) (map[KeyValueKey][]byte, []error) {
	ret := make(map[KeyValueKey][]byte)
	errs := make([]error, 0)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	pr := []byte(strings.Join(pre, g.DB.KV.D))
	for it.Seek(pr); it.ValidForPrefix(pr); it.Next() {
		item := it.Item()
		k := item.Key()
		v, err := item.ValueCopy(nil)
		if err != nil && err != badger.ErrKeyNotFound {
			errs = append(errs, err)
		} else {
			key := KeyValueKey{}
			err := key.Set(k, g.DB.KV.D, 4)
			if err != nil && err != badger.ErrKeyNotFound {
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
func (g *GetterFactory) getObjectMapArray(o *ObjectList) ([]map[string]interface{}, []error) {
	var errs []error
	var err error
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
		errs = append(errs, errors.New("can't get objects for type "+o.ObjectName))
		return nil, errs
	}
	ret := make([]map[string]interface{}, 0)
	g.DB.RLock()
	defer g.DB.RUnlock()
	ot, ok := g.DB.OT[o.ObjectName]
	if !ok {
		errs = append(errs, errors.New("can't find object of name "+o.ObjectName))
		return nil, errs
	}
	for _, val := range o.Objects {
		va := make(map[string]interface{})
		if o.fields == "" {
			for k, v := range val {
				if k.Main != "ID" {
					ft := ot.Fields[k.GetFullString(g.DB.KV.D)].FieldType
					if ft != "date" {
						va[k.GetFullString(g.DB.KV.D)], err = g.DB.FT[ft].Get(v)
					} else {
						va[k.GetFullString(g.DB.KV.D)] = string(v)
					}

					if err != nil {
						errs = append(errs, err)
					}
				} else {
					va[k.GetFullString(g.DB.KV.D)], err = g.DB.FT["uint64"].Get(v)
					if err != nil {
						errs = append(errs, err)
					}
				}
			}
		} else {
			fields := strings.Split(o.fields, ".")
			if fields[0] == "-" {
				fields = fields[1:]
				fieldsm := make(map[string]struct{})
				for _, v := range fields {
					fieldsm[v] = struct{}{}
				}
				for k, v := range val {
					if _, ok := fieldsm[k.GetFullString(g.DB.KV.D)]; k.Main != "ID" && !ok {
						ft := ot.Fields[k.GetFullString(g.DB.KV.D)].FieldType
						if ft != "date" {
							va[k.GetFullString(g.DB.KV.D)], err = g.DB.FT[ft].Get(v)
						} else {
							va[k.GetFullString(g.DB.KV.D)] = string(v)
						}
						if err != nil {
							errs = append(errs, err)
						}
					}
				}

			} else {
				for _, v := range fields {
					if rd, ok := val[KeyValueKey{Main: v}]; ok && v != "ID" {
						ft := ot.Fields[v].FieldType
						if ft == "" {

						} else if ft != "date" {
							va[v], err = g.DB.FT[ft].Get(rd)
						} else {
							va[v] = string(rd)
						}
						if err != nil {
							errs = append(errs, err)
						}
					}
				}
			}
			va["ID"], err = g.DB.FT["uint64"].Get(val[KeyValueKey{Main: "ID"}])
			if err != nil {
				errs = append(errs, err)
			}

		}
		ret = append(ret, va)

	}

	return ret, errs
}
func (g *GetterFactory) getLinkArray(o *LinkList) ([]interface{}, []error) {
	var errs []error
	if o.order.field != "" && !o.isIds {
		sort.Slice(o.Links, func(i, j int) bool {
			b := bytes.Compare(o.Links[i][KeyValueKey{Main: o.order.field}], o.Links[j][KeyValueKey{Main: o.order.field}])
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
		errs = append(errs, errors.New("Can't get Links for type "+o.LinkName))
		return nil, errs
	}
	ret := make([]interface{}, 0)
	g.DB.RLock()
	ot, ok := g.DB.LT[o.LinkName]
	g.DB.RUnlock()
	if !ok {
		errs = append(errs, errors.New("Can't find object of name "+o.LinkName))
		return nil, errs
	}

	for _, val := range o.Links {
		v, errs2 := ot.Get(val, g.DB)
		if len(errs2) > 0 {
			errs = append(errs, errs2...)

		} else {
			ret = append(ret, v)
		}

	}
	return ret, errs
}
func (g *GetterFactory) getLinkMapArray(o *LinkList) ([]map[string]interface{}, []error) {
	var errs []error
	var err error
	if o.order.field != "" && !o.isIds {
		sort.Slice(o.Links, func(i, j int) bool {
			b := bytes.Compare(o.Links[i][KeyValueKey{Main: o.order.field}], o.Links[j][KeyValueKey{Main: o.order.field}])
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
		errs = append(errs, errors.New("can't get Links for type "+o.LinkName))
		return nil, errs
	}
	ret := make([]map[string]interface{}, 0)
	g.DB.RLock()
	lt, ok := g.DB.LT[o.LinkName]
	if !ok {
		errs = append(errs, errors.New("can't find Link of name "+o.LinkName))
		return nil, errs
	}
	for _, val := range o.Links {
		va := make(map[string]interface{})
		for k, v := range val {
			if k.Main != "FROM" && k.Main != "TO" {
				va[k.GetFullString(g.DB.KV.D)], err = g.DB.FT[lt.Fields[k.GetFullString(g.DB.KV.D)].FieldType].Get(v)
				if err != nil {
					errs = append(errs, err)
				}
			} else {
				va[k.GetFullString(g.DB.KV.D)], err = g.DB.FT["uint64"].Get(v)
				if err != nil {
					errs = append(errs, err)
				}
			}
		}
		ret = append(ret, va)

	}
	g.DB.RUnlock()
	return ret, errs
}
func (g *GetterFactory) LoadObjects(txn *badger.Txn, node NodeQuery, isIds bool) (*ObjectList, []error) {
	ret := ObjectList{}
	ret.isIds = isIds
	ret.ObjectName = node.TypeName
	ret.order.field = node.Sort
	ret.order.typ = node.SortType
	ret.fields = node.Fields
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
		errs = append(errs, errors.New("Object Cannot load Link data"))
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
						prefix := g.DB.Options.DBName + g.DB.KV.D + node.TypeName + g.DB.KV.D + "ID" + g.DB.KV.D + string(id)
						_, err := txn.Get([]byte(prefix))
						if err != nil {
							errs = append(errs, err)
						}
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
	if node.Sort == "ID" && !node.SortType {
		iterator.reverse = true
	}
	errs = iterator.setup(g.DB, node.TypeName, node.index, node.Instructions[node.index], txn)
	defer iterator.close()
	delete(node.Instructions, node.index)
	for d, b, e := iterator.next(); b; d, b, e = iterator.next() {
		failed := false
		if e != nil {
			errs = append(errs, e)
		}
		if d != nil {
			for key, in1 := range node.Instructions {

				var fieldType FieldType
				var advancedFieldType AdvancedFieldType
				g.DB.RLock()
				va, ok := g.DB.OT[node.TypeName].Fields[key]
				if !ok {
					errs = append(errs, errors.New("Field name -"+key+"- not found in database"))
					g.DB.RUnlock()
					return &ret, errs
				} else {
					if !va.Advanced {
						fieldType = g.DB.FT[va.FieldType]
					} else {
						advancedFieldType = g.DB.AFT[va.FieldType]
					}
				}
				g.DB.RUnlock()

				if !va.Advanced {
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
						Log.Error().Interface("error", err).Interface("stack", string(debug.Stack())).Msg("Getting value for key in Badger threw error again")
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
								Log.Error().Interface("error", err).Str("key", string(item.KeyCopy(nil))).Interface("stack", string(debug.Stack())).Msg("Getting value for key after getting item in Badger threw error")
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
				} else {
					for _, in2 := range in1 {
						ok, err := advancedFieldType.Compare(txn, g.DB, true, node.TypeName, d[KeyValueKey{Main: "ID"}], d[KeyValueKey{Main: "ID"}], key, in2.action, in2.param)
						if len(err) >= 1 {
							errs = append(errs, err...)
							return &ret, errs
						}
						if !ok {
							failed = true
						}
					}
				}

			} // end of for loop
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
	ret.fields = node.Fields
	objectSkips := 0
	objectCount := 0
	var errs []error
	if node.limit == 0 {
		node.limit = 100
	}
	if isIds {
		ret.IDs = make([]LinkListList, 0)
	} else {
		ret.Links = make([]map[KeyValueKey][]byte, 0)
	}
	if node.Direction == "" {
		errs = append(errs, errors.New("this NodeQuery object is not for loading links"))
		return nil, errs
	}
	g.DB.RLock()
	_, ok := g.DB.LT[ret.LinkName]
	g.DB.RUnlock()
	if !ok {
		errs = append(errs, errors.New("LinkType '"+ret.LinkName+"' not found in database"))
		return nil, errs
	}
	if node.Direction == "-" {
		errs = append(errs, errors.New("you can't load links of type '-'"))
		return nil, errs
	}
	ins, ok := node.Instructions["FROM"]
	ins2, ok2 := node.Instructions["TO"]
	if ok && ok2 {

		var to, from uint64
		for _, query := range ins {
			if query.action == "==" {
				from, ok = query.param.(uint64)
				if !ok {
					errs = append(errs, errors.New("invalid argument provided in nodequery"))
				}
			}
		}
		for _, query := range ins2 {
			if query.action == "==" {
				to, ok = query.param.(uint64)
				if !ok {
					errs = append(errs, errors.New("invalid argument provided in nodequery"))
				}
			}
		}
		if from != uint64(0) && to != uint64(0) {

			g.DB.RLock()
			f, err := g.DB.FT["uint64"].Set(from)
			if err != nil {
				errs = append(errs, err)
			}
			t, err := g.DB.FT["uint64"].Set(to)
			if err != nil {
				errs = append(errs, err)
			}
			g.DB.RUnlock()
			indexed := ""
			if node.Direction == "->" {
				indexed = "+"
			} else {
				indexed = "-"
			}
			prefix := g.DB.Options.DBName + g.DB.KV.D + node.TypeName + g.DB.KV.D + indexed + g.DB.KV.D + string(f) + g.DB.KV.D + string(t)
			_, err = txn.Get([]byte(prefix))
			if err != nil {
				errs = append(errs, err)
				return &ret, errs
			}
			if isIds {
				ret.IDs = append(ret.IDs, LinkListList{FROM: f, TO: t})
				return &ret, errs
			}

			var obj map[KeyValueKey][]byte
			var ers []error
			if node.Direction == "<-" {
				obj, ers = g.getKeysWithValueLinks(txn, g.DB.Options.DBName, node.TypeName, string(t), string(f))
			} else {
				obj, ers = g.getKeysWithValueLinks(txn, g.DB.Options.DBName, node.TypeName, string(f), string(t))
			}
			if len(ers) > 0 {
				errs = append(errs, ers...)
			}
			if obj != nil {
				obj[KeyValueKey{Main: "FROM"}] = f
				obj[KeyValueKey{Main: "TO"}] = t
				ret.Links = append(ret.Links, obj)
				return &ret, errs
			}
		} else {
			errs = append(errs, errors.New("TO and FROM provided is invalid"))
			return &ret, errs
		}

	}

	if ok && !ok2 {
		skipped := 0
		count := 0
		var from uint64
		for _, query := range ins {
			if query.action == "==" {
				from, ok = query.param.(uint64)
				if !ok {
					errs = append(errs, errors.New("invalid argument provided in nodequery"))
				}
			}
		}

		if from != uint64(0) {
			g.DB.RLock()
			f, err := g.DB.FT["uint64"].Set(from)

			g.DB.RUnlock()
			if err != nil {
				errs = append(errs, err)
			}
			indexed := ""
			if node.Direction == "->" {
				indexed = "+"
			} else {
				indexed = "-"
			}
			prefix := g.DB.Options.DBName + g.DB.KV.D + node.TypeName + g.DB.KV.D + indexed + g.DB.KV.D + string(f)

			opt := badger.DefaultIteratorOptions
			opt.Prefix = []byte(prefix)
			opt.PrefetchSize = 20
			opt.PrefetchValues = false
			iterator := txn.NewIterator(opt)
			defer iterator.Close()
			brk := false
			iterator.Seek([]byte(prefix))

			for !brk {
				if !iterator.ValidForPrefix([]byte(prefix)) {
					iterator.Close()
					return &ret, errs
				}
				item := iterator.Item()
				var key []byte
				key = item.KeyCopy(key)
				kArray := bytes.Split(key, []byte(g.DB.KV.D))
				if isIds {
					ret.IDs = append(ret.IDs, LinkListList{FROM: f, TO: kArray[4]})
				} else {
					g.DB.RLock()
					rawFrom, er := g.DB.FT["uint64"].Set(from)
					g.DB.RUnlock()
					if er != nil {
						errs = append(errs, er)
					} else {
						var obj map[KeyValueKey][]byte
						var err []error
						if node.Direction == "<-" {
							obj, err = g.getKeysWithValueLinks(txn, g.DB.Options.DBName, node.TypeName, string(rawFrom), string(kArray[4]))
						} else {
							obj, err = g.getKeysWithValueLinks(txn, g.DB.Options.DBName, node.TypeName, string(rawFrom), string(kArray[4]))
						}
						if len(err) > 0 {
							errs = append(errs, err...)
						}

						if obj != nil {
							if node.skip > skipped {
								skipped++
							} else {
								obj[KeyValueKey{Main: "FROM"}] = rawFrom
								obj[KeyValueKey{Main: "TO"}] = kArray[4]
								ret.Links = append(ret.Links, obj)
								count++
							}
							if count >= node.limit {
								iterator.Close()
								return &ret, errs
							}

						}
					}
				}
				iterator.Next()
			}
		} else {
			errs = append(errs, errors.New("invalid ID privided in Node query"))
		}

	}

	if !ok && ok2 {
		errs = append(errs, errors.New("you can't run a nodequery for links with field 'TO' without field 'FROM' instead run the query with a FROM and direction '<-'"))
		return &ret, errs
	}

	var prefix []byte
	if node.Direction == "<-" {
		prefix = []byte(g.DB.Options.DBName + g.DB.KV.D + node.TypeName + g.DB.KV.D + "-")
	} else if node.Direction == "->" {
		prefix = []byte(g.DB.Options.DBName + g.DB.KV.D + node.TypeName + g.DB.KV.D + "+")
	} else if node.Direction == "-" {
		prefix = []byte(g.DB.Options.DBName + g.DB.KV.D + node.TypeName + g.DB.KV.D + "+")
	}
	opt := badger.DefaultIteratorOptions
	opt.Prefix = prefix
	opt.PrefetchSize = 20
	opt.PrefetchValues = false
	iterator := txn.NewIterator(opt)
	defer iterator.Close()
	brk := false
	iterator.Seek(prefix)
	for !brk {
		if !iterator.ValidForPrefix(prefix) {
			iterator.Close()
			return &ret, errs
		}
		d := make(map[KeyValueKey][]byte)
		item := iterator.Item()
		var key []byte
		key = item.KeyCopy(key)

		kArray := bytes.Split(key, []byte(g.DB.KV.D))
		failed := false
		if len(node.Instructions) < 1 {
			failed = false
		}
		for key, in1 := range node.Instructions {
			var advancedFieldType AdvancedFieldType
			var fieldType FieldType
			var va FieldOptions
			g.DB.RLock()
			va, ok := g.DB.LT[node.TypeName].Fields[key]
			if !ok {
				errs = append(errs, errors.New("Field name -"+key+"- not found in database"))
				iterator.Close()
				g.DB.RUnlock()
				return &ret, errs
			}
			if !va.Advanced {
				fieldType = g.DB.FT[va.FieldType]
			} else {
				advancedFieldType = g.DB.AFT[va.FieldType]
			}
			g.DB.RUnlock()

			if !va.Advanced {
				var buffer bytes.Buffer
				buffer.WriteString(g.DB.Options.DBName)
				buffer.WriteString(g.DB.KV.D)
				buffer.WriteString(node.TypeName)
				buffer.WriteString(g.DB.KV.D)
				if node.Direction == "<-" {
					buffer.Write(kArray[4])
				} else {
					buffer.Write(kArray[3])
				}
				buffer.WriteString(g.DB.KV.D)
				if node.Direction == "<-" {
					buffer.Write(kArray[3])
				} else {
					buffer.Write(kArray[4])
				}
				buffer.WriteString(g.DB.KV.D)
				buffer.WriteString(key)
				item2, err := txn.Get(buffer.Bytes())
				if err != nil && err == badger.ErrKeyNotFound {
				} else if err != nil && err != badger.ErrKeyNotFound {
					Log.Error().Interface("error", err).Interface("stack", string(debug.Stack())).Msg("Getting value for key in Badger threw error again")
					errs = append(errs, err)
					iterator.Close()
					return &ret, errs
				} else if err == nil && item2 != nil {
					var val []byte
					for _, in2 := range in1 {
						rawParam, err := fieldType.Set(in2.param)
						if err != nil {
							errs = append(errs, err)
							return &ret, errs
						}
						val, err = item2.ValueCopy(nil)
						if err != nil {
							Log.Error().Interface("error", err).Str("key", string(item.KeyCopy(nil))).Interface("stack", string(debug.Stack())).Msg("Getting value for key after getting item in Badger threw error")
							iterator.Close()
							errs = append(errs, err)
							return &ret, errs
						}
						ok, err := fieldType.Compare(in2.action, val, rawParam)
						if err != nil {
							errs = append(errs, err)
							iterator.Close()
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
			} else {
				for _, in2 := range in1 {
					var ok bool
					var err []error
					if node.Direction == "<-" {
						ok, err = advancedFieldType.Compare(txn, g.DB, false, node.TypeName, kArray[4], kArray[3], key, in2.action, in2.param)
					} else {
						ok, err = advancedFieldType.Compare(txn, g.DB, false, node.TypeName, kArray[3], kArray[4], key, in2.action, in2.param)
					}
					if len(err) > 0 {
						errs = append(errs, err...)
						return &ret, errs
					}
					if !ok {
						failed = true
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
					ret.IDs = append(ret.IDs, LinkListList{kArray[3], kArray[4]})
				} else {
					notIncluded := make([]string, 0)
					g.DB.RLock()
					for k := range g.DB.LT[node.TypeName].Fields {
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
						if node.Direction == "<-" {
							buffer.Write(kArray[4])
						} else {
							buffer.Write(kArray[3])
						}
						buffer.WriteString(g.DB.KV.D)
						if node.Direction == "<-" {
							buffer.Write(kArray[3])
						} else {
							buffer.Write(kArray[4])
						}
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
					d[KeyValueKey{Main: "FROM"}] = kArray[3]
					d[KeyValueKey{Main: "TO"}] = kArray[4]
					ret.Links = append(ret.Links, d)
				}
				objectCount++
			}

		}
		iterator.Next()
	}

	return &ret, errs
}
func (g *GetterFactory) Start(db *DB, numOfRuners int, inputChannelLength int) {
	g.DB = db
	g.Input = make(chan Query, inputChannelLength)
	for i := 0; i < numOfRuners; i++ {
		go g.Run()
	}
}
func (g *GetterFactory) Run() {
	var job Query
	//var ret GetterRet
	var data map[string]interface{}
	var txn *badger.Txn
	defer func() {
		r := recover()
		if r != nil {
			ret := GetterRet{}
			Log.Error().Interface("recovered", r).Stack().Interface("stack", string(debug.Stack())).Msg("Recovered in Getter.Run ")
			if ret.Errors == nil {
				ret.Errors = make([]error, 0)
			}
			switch x := r.(type) {
			case string:
				ret.Errors = append(ret.Errors, errors.New(x))
			case error:
				ret.Errors = append(ret.Errors, x)
			default:
				ret.Errors = append(ret.Errors, errors.New("unknown error was thrown"))
			}
			if job.Ret != nil {
				job.Ret <- ret
				close(job.Ret)
			}
			if txn != nil {
				func() {
					defer func() {
						r := recover()
						if r != nil {
							ret := GetterRet{}
							Log.Error().Interface("recovered", r).Stack().Interface("stack", string(debug.Stack())).Msg("Recovered in Getter.Run.recover.recover ")
							if ret.Errors == nil {
								ret.Errors = make([]error, 0)
							}
							switch x := r.(type) {
							case string:
								ret.Errors = append(ret.Errors, errors.New(x))
							case error:
								ret.Errors = append(ret.Errors, x)
							default:
								ret.Errors = append(ret.Errors, errors.New("unknown error was thrown"))
							}
						}
					}()
					txn.Discard()
				}()
				txn = nil
			}
			g.Run()
		}
	}()

	for {
		job = <-g.Input
		ret := GetterRet{}
		ret.Errors = make([]error, 0)
		data = make(map[string]interface{})
		if txn != nil {
			txn.Discard()
		}
		txn = g.DB.KV.DB.NewTransaction(false)
		for _, val := range job.Instructions {
			switch val.Action {
			case "return":
				GetterReturn(g, txn, &data, &job, val.Params, &ret)
			case "return.map":
				GetterReturnMap(g, txn, &data, &job, val.Params, &ret)
			case "object.new":
				GetterNewObject(g, txn, &data, &job, val.Params, &ret)
			case "object":
				GetterObjects(g, txn, &data, &job, val.Params, &ret)
			case "link.new":
				GetterNewLink(g, txn, &data, &job, val.Params, &ret)
			case "merge.objectlist":
				MergeObjectList(g, txn, &data, &job, val.Params, &ret)
			case "link":
				GetterLinks(g, txn, &data, &job, val.Params, &ret)
			case "graph.p":
				GetterGraphPartern(g, txn, &data, &job, val.Params, &ret)
			case "graph.p.o":
				GetterGraphParternObjectStart(g, txn, &data, &job, val.Params, &ret)
			case "graph.s":
				GetterGraphStraight(g, txn, &data, &job, val.Params, &ret)
			case "graph.s.o":
				GetterGraphStraightObjectStart(g, txn, &data, &job, val.Params, &ret)
			case "embeded":
				GetterGraphEmbeded(g, txn, &data, &job, val.Params, &ret)
			case "internal.instruction":
				InternalInstruction(g, txn, &data, &job, val.Params, &ret)
			default:
				g.DB.RLock()
				f, ok := g.DB.GF[val.Action]
				g.DB.RUnlock()
				if !ok {
					ret.Errors = append(ret.Errors, errors.New("Invalid Istruction in GetterFactory: "+val.Action))
				} else {
					f(g, txn, &data, &job, val.Params, &ret)
				}
			}

		}

	}
}
func (g *GetterFactory) Close() {
	for {
		if len(g.Input) == 0 {
			break
		}
	}
}

//GetterNewObject ..
//action: 'object.new'
//params [0] Object name (String)
//return New object
func GetterNewObject(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	g.DB.RLock()
	one, ok := qData[0].(string)
	if !ok {
		ret.Errors = append(ret.Errors, errors.New("invalid argument provided in nodequery"))
	}
	ot, ok := g.DB.OT[one]
	g.DB.RUnlock()
	if !ok {
		ret.Errors = append(ret.Errors, errors.New("object Type is not saved in the Database"))
	}
	ret.Data = ot.New(g.DB)
	q.Ret <- *ret
}

//GetterNewObject ..
//action: 'object.new'
//params [0] Object name (String)
//return New object
func MergeObjectList(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	if len(qData) < 3 {
		ret.Errors = append(ret.Errors, errors.New("less than three arguments given in mergeobjectlist"))
		return
	}
	one, ok := qData[0].(string)
	if !ok {
		ret.Errors = append(ret.Errors, errors.New("invalid first argument provided in mergeobjectlist"))
		return
	}

	two, ok := qData[1].(string)
	if !ok {
		ret.Errors = append(ret.Errors, errors.New("invalid second argument provided in mergeobjectlist"))
		return
	}

	result, ok := qData[2].(string)
	if !ok {
		ret.Errors = append(ret.Errors, errors.New("invalid third argument provided in mergeobjectlist"))
		return
	}
	d1, ok := (*data)[one].(*ObjectList)
	if !ok {
		ret.Errors = append(ret.Errors, errors.New("no objectlist with the provided name of the first argument in mergeobjectlist"))
		return
	}

	d2, ok := (*data)[two].(*ObjectList)
	if !ok {
		ret.Errors = append(ret.Errors, errors.New("no objectlist with the provided name of the second argument in mergeobjectlist"))
		return
	}
	if d1.ObjectName != d2.ObjectName {
		ret.Errors = append(ret.Errors, errors.New("provided objects to merge are not the same in mergeobjectlist"))
		return
	}

	re := *d1
	re.Objects = make([]map[KeyValueKey][]byte, 0) // append(d1.Objects, d2.Objects...)

	t := make(map[string]struct{})

	for _, v := range d1.Objects {
		if _, ok := t[string(v[KeyValueKey{Main: "ID"}])]; !ok {
			t[string(v[KeyValueKey{Main: "ID"}])] = struct{}{}
			re.Objects = append(re.Objects, v)
		}
	}

	for _, v := range d2.Objects {
		if _, ok := t[string(v[KeyValueKey{Main: "ID"}])]; !ok {
			t[string(v[KeyValueKey{Main: "ID"}])] = struct{}{}
			re.Objects = append(re.Objects, v)
		}
	}

	(*data)[result] = &re

}

//GetterObjects ..
//action: 'objects'
//params [0] NodeQuery (NodeQuery)
//params [1] Object name (String)
//placesses objectLists found in node query [0] in variable [1]
func GetterObjects(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	n, bul := qData[0].(NodeQuery)
	if !bul {
		ret.Errors = append(ret.Errors, errors.New("invalid NodeQuery provided"))
		return
	}
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
		d, ok := qData[0].(string)
		if !ok {
			ret.Errors = append(ret.Errors, errors.New("invalid argument provided in return function"))
		}
		da := (*data)[d]
		switch da.(type) {
		case *ObjectList:
			d2 := da.(*ObjectList)
			if d2 != nil {
				r, errs := g.getObjectArray(da.(*ObjectList))
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
				}
				if len(r) > 0 {
					d2 := qData[1].(int)
					if !ok {
						ret.Errors = append(ret.Errors, errors.New("invalid argument provided in return function"))
					}
					ret.Data = r[d2]
				}
			}
		case *LinkList:
			d2 := da.(*LinkList)
			if d2 != nil {
				r, errs := g.getLinkArray(da.(*LinkList))
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
				}
				if len(r) > 0 {
					d2 := qData[1].(int)
					if !ok {
						ret.Errors = append(ret.Errors, errors.New("invalid argument provided in return function"))
					}
					ret.Data = r[d2]
				}
			}
		default:
			d2, ok := qData[0].(string)
			if !ok {
				ret.Errors = append(ret.Errors, errors.New("invalid argument provided in return function"))
			}
			ret.Data = (*data)[d2]

		}
	}
	if q.ReturnType == 2 {
		d3, ok := qData[0].(string)
		if !ok {
			ret.Errors = append(ret.Errors, errors.New("invalid argument provided in return function"))
		}
		da := (*data)[d3]

		switch d2 := da.(type) {
		case *ObjectList:
			if d2 != nil {
				r, errs := g.getObjectArray(d2)
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
				}
				if r != nil {
					ret.Data = r
				}
			}
		case *LinkList:
			if d2 != nil {
				r, errs := g.getLinkArray(d2)
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
				}
				if r != nil {
					ret.Data = r
				}
			}

		}
	}
	if q.ReturnType == 3 {
		returned := make(map[string]interface{})
		for _, val := range qData {
			do, ok := val.(string)
			if !ok {
				ret.Errors = append(ret.Errors, errors.New("invalid argument provided in return function"))
			}

			da, ok := (*data)[do]
			if ok {
				switch d2 := da.(type) {
				case *ObjectList:
					if d2 != nil {
						r, errs := g.getObjectArray(d2)
						if len(errs) > 0 {
							ret.Errors = append(ret.Errors, errs...)
						}
						if r != nil {
							returned[do] = r
						}
					}
				case *LinkList:
					if d2 != nil {
						r, errs := g.getLinkArray(d2)
						if len(errs) > 0 {
							ret.Errors = append(ret.Errors, errs...)
						}
						if r != nil {
							returned[do] = r
						}
					}
				default:
					returned[do] = (*data)[do]
				}
			} else {
				ret.Errors = append(ret.Errors, errors.New("A return value expected is not returned -'"+do+"'-"))
			}

		}
		ret.Data = returned
	}
	q.Ret <- *ret
}

//GetterReturnMap ..
//action: 'return'
// for returntype 1. params [0] Object name (String), params [1] Index of the objects to return (int)
//return interface{}
func GetterReturnMap(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {

	d, ok := qData[0].(string)
	if !ok {
		ret.Errors = append(ret.Errors, errors.New("invalid first argument provided in return.map function"))
	}
	switch d {
	case "single":
		//q[1] = field to get
		//q[2] := index
		d2, ok := qData[1].(string)
		if !ok {
			ret.Errors = append(ret.Errors, errors.New("invalid second argument provided in return map function"))
		}
		da := (*data)[d2]
		switch x := da.(type) {
		case *ObjectList:
			if x != nil {
				r, errs := g.getObjectMapArray(x)
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
				}
				if len(r) > 0 {
					d3 := qData[2].(int)
					if !ok {
						ret.Errors = append(ret.Errors, errors.New("invalid index argument provided in return map function"))
					}
					ret.Data = r[d3]
				}
			}
		case *LinkList:
			if x != nil {
				r, errs := g.getLinkMapArray(x)
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
				}
				if len(r) > 0 {
					d3 := qData[2].(int)
					if !ok {
						ret.Errors = append(ret.Errors, errors.New("invalid index argument provided in return map function"))
					}
					ret.Data = r[d3]
				}
			}
		default:
			ret.Data = (*data)[d2]

		}
	case "array":
		//q[1] = field to get
		d1, ok := qData[1].(string)
		if !ok {
			ret.Errors = append(ret.Errors, errors.New("invalid first argument provided in return map function"))
		}
		da := (*data)[d1]

		switch x := da.(type) {
		case *ObjectList:
			if x != nil {
				r, errs := g.getObjectMapArray(x)
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
				}
				if r != nil {
					ret.Data = r
				}
			}
		case *LinkList:
			if x != nil {
				r, errs := g.getLinkMapArray(x)
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
				}
				if r != nil {
					ret.Data = r
				}
			}

		}
	case "map":
		returned := make(map[string]interface{})
		for k, val := range qData {
			if k != 0 {
				do, ok := val.(string)
				if !ok {
					ret.Errors = append(ret.Errors, errors.New("invalid argument provided in return function"))
				}

				da, ok := (*data)[do]
				if ok {
					switch x := da.(type) {
					case *ObjectList:
						if x != nil {
							r, errs := g.getObjectMapArray(x)
							if len(errs) > 0 {
								ret.Errors = append(ret.Errors, errs...)
							}
							if r != nil {
								returned[do] = r
							}
						}
					case *LinkList:
						if x != nil {
							r, errs := g.getLinkMapArray(x)
							if len(errs) > 0 {
								ret.Errors = append(ret.Errors, errs...)
							}
							if r != nil {
								returned[do] = r
							}
						}
					default:
						returned[do] = (*data)[do]
					}
				} else {
					ret.Errors = append(ret.Errors, errors.New("a return value expected is not returned -'"+do+"'-"))
				}
			}
		}
		ret.Data = returned
	default:
		ret.Errors = append(ret.Errors, errors.New("invalid first argument provided in return.map function, expecting 'single', 'multiple' or 'map'"))
	}
	q.Ret <- *ret
}

//GetterNewLink ..
//action: 'link.new'
//params [0] Link name (String)
//return New link
func GetterNewLink(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	st, bul := qData[0].(string)
	if !bul {
		ret.Errors = append(ret.Errors, errors.New("invalid NodeQuery provided"))
		return
	}
	g.DB.RLock()
	lt, ok := g.DB.LT[st]
	g.DB.RUnlock()
	if !ok {
		ret.Errors = append(ret.Errors, errors.New("Object Type is not saved in the Database"))
	}
	ret.Data = lt.New(g.DB)
	q.Ret <- *ret
}

//GetterLinks ..
//action: 'objects'
//params [0] NodeQuery (NodeQuery)
//params [1] Object name (String)
//placesses LinkLists found in node query [0] in variable [1]
func GetterLinks(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	n, bul := qData[0].(NodeQuery)
	if !bul {
		ret.Errors = append(ret.Errors, errors.New("invalid NodeQuery provided"))
		return
	}
	obs, errs := g.LoadLinks(txn, n, false)
	if len(errs) > 0 {
		ret.Errors = append(ret.Errors, errs...)
	}
	(*data)[n.saveName] = obs
}

type iteratorLoaderGraphStart struct {
	node      *NodeQuery
	g         *GetterFactory
	txn       *badger.Txn
	prefix    string
	prefix2   string
	fieldType FieldType
	query     []NodeQueryInstruction
	iterator  *badger.Iterator
	field     string
	obj       string
	indexed   bool
	reverse   bool
	gottenID  bool
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

func (i *iteratorLoaderGraphStart) setup(g *GetterFactory, node *NodeQuery, txn *badger.Txn) []error {
	obj := node.TypeName
	field := node.index

	if field == "ID" {
		field = ""
	}

	inst := node.Instructions[node.index]
	var errs []error
	g.DB.RLock()
	vaa, ok := g.DB.OT[obj].Fields[field]
	if !ok {
		errs = append(errs, errors.New("Field -"+field+"- Not found for object -"+obj+"- in the Database"))
	}
	i.indexed = vaa.Indexed
	i.fieldType = g.DB.FT[g.DB.OT[obj].Fields[field].FieldType]
	g.DB.RUnlock()
	i.field = field
	i.txn = txn
	i.obj = obj
	i.g = g

	i.node = node
	index := ""

	delete(node.Instructions, field)
	//i.iterator = txn.NewIterator()
	if i.indexed && len(inst) > 0 {
		val, ins, err := i.fieldType.CompareIndexed(inst[0].action, inst[0].param)
		if err != nil {
			errs = append(errs, err)
		}
		index = val
		switch ins {
		case "==":
			i.center.ins = ins
			i.center.val = val
		case "<=":
			i.left.ins = ins
			i.left.val = val
			i.reverse = true
		case "<":
			i.left.ins = ins
			i.left.val = val
			i.reverse = true
		case ">=":
			i.right.ins = ins
			i.right.val = val
		case ">":
			i.right.ins = ins
			i.right.val = val
		case "prefix":
			i.center.ins = ins
			i.center.val = val
		}
		i.query = inst[1:]

		i.prefix = g.DB.Options.DBName + g.DB.KV.D + obj + g.DB.KV.D + field + g.DB.KV.D + index
		i.prefix2 = g.DB.Options.DBName + g.DB.KV.D + obj + g.DB.KV.D + field
		opt := badger.DefaultIteratorOptions
		opt.Prefix = []byte(i.prefix)
		opt.PrefetchSize = 20
		opt.Reverse = i.reverse
		opt.PrefetchValues = false
		i.iterator = txn.NewIterator(opt)
	} else {
		i.prefix = g.DB.Options.DBName + g.DB.KV.D + obj + g.DB.KV.D + "ID"
		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false
		opt.Prefix = []byte(i.prefix)
		opt.PrefetchSize = 20
		if node.Sort == "ID" && !node.SortType {
			opt.Reverse = true
		}
		i.iterator = txn.NewIterator(opt)
		i.query = inst
	}
	i.iterator.Seek([]byte(i.prefix))
	return errs
}
func (i *iteratorLoaderGraphStart) close() {
	if i.iterator != nil {
		i.iterator.Close()
	}
}
func (i *iteratorLoaderGraphStart) next() (map[KeyValueKey][]byte, bool, error) {
	r := make(map[KeyValueKey][]byte)
	var k []byte
	var kArray [][]byte
	notValid := true

	for notValid {

		item, err := getItem(i.iterator)
		if err != nil {
			return r, false, nil
		}
		k = item.KeyCopy(k)
		kArray = bytes.Split(k, []byte(i.g.DB.KV.D))
		if i.indexed {
			// check if the key is for this field, if not go to the next one and check again, if test faild 2 times return
			if string(kArray[2]) != i.field || string(kArray[1]) != i.obj {
				i.iterator.Next()
				item, err = getItem(i.iterator)
				if err != nil {
					return r, false, nil
				}
				k = item.KeyCopy(k)
				kArray = bytes.Split(k, []byte(i.g.DB.KV.D))
				if string(kArray[2]) != i.field || string(kArray[1]) != i.obj {
					return r, false, nil
				}

			}

			failed := false
			//check indexed indexed instruction
			if i.center.ins != "" {
				isValid := false
				if i.center.ins == "==" {
					isValid = bytes.Equal(kArray[3], []byte(i.center.val))
				} else if i.center.ins == "prefix" {
					isValid = strings.HasPrefix(string(kArray[3]), i.center.val)
				}
				if !isValid {
					failed = true
				}
			} else if i.left.ins != "" {
				d := bytes.Compare(kArray[3], []byte(i.left.val))
				if i.left.ins == "<" && d != -1 {
					failed = true
				}
				if i.left.ins == "<=" && d > 0 {
					failed = true
				}
			} else if i.right.ins != "" {
				d := bytes.Compare(kArray[3], []byte(i.left.val))
				if i.right.ins == ">" && d != 1 {
					failed = true
				}
				if i.right.ins == ">=" && d < 0 {
					failed = true
				}
			}

			//check other none indexed instructions
			for _, ins := range i.query {
				rawQueryData, err := i.fieldType.Set(ins.param)
				if err != nil {
					return nil, false, err
				}
				boa, err := i.fieldType.Compare(ins.action, kArray[3], rawQueryData)
				if err != nil {
					return nil, false, err
				}
				if !boa {
					failed = true
				}
			}

			if !failed {
				notValid = false
				r[KeyValueKey{Main: string(kArray[2])}] = kArray[3]
				r[KeyValueKey{Main: "ID"}] = kArray[4]
			} else {
				i.iterator.Next()
			}

		} else if !i.indexed && i.field == "" {
			if !i.iterator.ValidForPrefix([]byte(i.prefix)) {
				return r, false, nil
			}
			notValid = false
			r[KeyValueKey{Main: "ID"}] = kArray[3]

		} else if !i.indexed && i.field != "" {
			if !i.iterator.ValidForPrefix([]byte(i.prefix)) {
				return r, false, nil
			}
			var v []byte
			var buffer bytes.Buffer
			buffer.WriteString(i.g.DB.Options.DBName)
			buffer.WriteString(i.g.DB.KV.D)
			buffer.WriteString(i.obj)
			buffer.WriteString(i.g.DB.KV.D)
			buffer.Write(kArray[3])
			buffer.WriteString(i.g.DB.KV.D)
			buffer.WriteString(i.field)
			item2, err := i.txn.Get(buffer.Bytes())
			if err != nil && err != badger.ErrKeyNotFound {
				Log.Error().Interface("error", err).Interface("stack", string(debug.Stack())).Str("key", string(k)).Msg("Getting value for key in Badger threw error")
				return nil, false, err
			} else if item2 == nil {
			} else {
				v, err = item2.ValueCopy(v)
				if err != nil {
					Log.Error().Interface("error", err).Interface("stack", string(debug.Stack())).Str("key", buffer.String()).Msg("Getting value for key in Badger threw error")
					return nil, false, err
				}

				boa := true
				for _, ins := range i.query {
					rawQueryData, err := i.fieldType.Set(ins.param)
					if err != nil {
						return nil, false, err
					}
					oa, err := i.fieldType.Compare(ins.action, v, rawQueryData)
					if err != nil {
						return nil, false, err
					}
					if !oa {
						boa = false
					}
					if !boa {
						break
					}
				}
				if boa {
					notValid = false
					others := ""
					ks := string(item2.KeyCopy(nil))
					ksa := strings.Split(ks, i.g.DB.KV.D)
					others = strings.Join(ksa[4:], i.g.DB.KV.D)
					r[KeyValueKey{Main: i.field, Subs: others}] = v
					r[KeyValueKey{Main: "ID"}] = kArray[3]

				}
			}

		}
		i.iterator.Next()
	}

	return r, true, nil
}
func (i *iteratorLoaderGraphStart) next2() (a map[KeyValueKey][]byte, b []byte, c bool, e []error) {
	if i.node.saveName == "" {
		b = make([]byte, 0)
	} else {
		a = make(map[KeyValueKey][]byte)
	}
	if i.node.Direction != "" {
		e = append(e, errors.New("Object Cannot load Link data"))
		return
	}
	ins, ok := i.node.Instructions["ID"]
	if ok {
		for _, query := range ins {
			if query.action == "==" {
				if i.gottenID {
					c = false
					return
				}

				// just return an object list with the id requested
				i.g.DB.RLock()
				id, err := i.g.DB.FT["uint64"].Set(query.param)
				i.g.DB.RUnlock()
				if err != nil {
					e = append(e, err)
					return
				}

				var buffer bytes.Buffer
				buffer.WriteString(i.g.DB.Options.DBName)
				buffer.WriteString(i.g.DB.KV.D)
				buffer.WriteString(i.node.TypeName)
				buffer.WriteString(i.g.DB.KV.D)
				buffer.WriteString("ID")
				buffer.WriteString(i.g.DB.KV.D)
				buffer.Write(id)
				buffer.WriteString(i.g.DB.KV.D)
				buffer.Write(id)
				_, err = i.txn.Get(buffer.Bytes())
				if err != nil && err != badger.ErrKeyNotFound {
					e = append(e, err)
					return
				}

				if err != nil && err == badger.ErrKeyNotFound {
					c = false
					return
				}

				i.gottenID = true
				c = true

				if i.node.saveName == "" {
					b = id
				} else {
					i.g.DB.RLock()
					rawID, er := i.g.DB.FT["uint64"].Set(query.param)
					i.g.DB.RUnlock()
					if er != nil {
						e = append(e, er)
					} else {
						obj, err := i.g.getKeysWithValue(i.txn, i.g.DB.Options.DBName, i.node.TypeName, string(rawID))
						if len(err) > 0 {
							e = append(e, err...)
						}
						if obj != nil {
							obj[KeyValueKey{Main: "ID"}] = rawID
							a = obj
							c = true
						}
					}
				}
				return
			}

		}
	}

	iterator := i
	do := true
	//errs = iterator.setup(g.DB, node.TypeName, node.index, node.Instructions[node.index], txn)
	for do {
		d, j, errs := iterator.next()
		failed := false
		if e != nil {
			e = append(e, errs)
		}
		if !j {
			do = false
			c = false
			return
		}
		if d != nil {
			for key, in1 := range i.node.Instructions {
				var fieldType FieldType
				var advancedFieldType AdvancedFieldType
				var va FieldOptions
				i.g.DB.RLock()
				va, ok = i.g.DB.OT[i.node.TypeName].Fields[key]
				if !ok {
					e = append(e, errors.New("Field name -"+key+"- not found in database"))
					i.g.DB.RUnlock()
					return
				}
				if !va.Advanced {
					fieldType = i.g.DB.FT[va.FieldType]
				} else {
					advancedFieldType = i.g.DB.AFT[va.FieldType]
				}
				i.g.DB.RUnlock()
				if !va.Advanced {
					var buffer bytes.Buffer
					buffer.WriteString(i.g.DB.Options.DBName)
					buffer.WriteString(i.g.DB.KV.D)
					buffer.WriteString(i.node.TypeName)
					buffer.WriteString(i.g.DB.KV.D)
					buffer.Write(d[KeyValueKey{Main: "ID"}])
					buffer.WriteString(i.g.DB.KV.D)
					buffer.WriteString(key)
					item, err := i.txn.Get(buffer.Bytes())
					if err != nil && err == badger.ErrKeyNotFound {
						failed = true
					} else if err != nil && err != badger.ErrKeyNotFound {
						Log.Error().Interface("error", err).Interface("stack", string(debug.Stack())).Msg("Getting value for key in Badger threw error again")
						e = append(e, err)
						return
					} else if err == nil && item != nil {
						var val []byte
						for _, in2 := range in1 {
							rawParam, err := fieldType.Set(in2.param)
							if err != nil {
								e = append(e, err)
								return
							}
							val, err = item.ValueCopy(nil)
							if err != nil {
								Log.Error().Interface("error", err).Str("key", string(item.KeyCopy(nil))).Interface("stack", string(debug.Stack())).Msg("Getting value for key after getting item in Badger threw error")
								e = append(e, err)
								return
							}
							ok, err := fieldType.Compare(in2.action, val, rawParam)
							if err != nil {
								e = append(e, err)
								return
							}
							if !ok {
								failed = true
							}
						}
						if !failed {
							if i.node.saveName != "" {
								d[KeyValueKey{Main: key}] = val
							}
						}
					}
				} else {
					for _, in2 := range in1 {
						ok, err := advancedFieldType.Compare(i.txn, i.g.DB, true, i.node.TypeName, d[KeyValueKey{Main: "ID"}], d[KeyValueKey{Main: "ID"}], key, in2.action, in2.param)
						if len(err) >= 1 {
							e = append(e, err...)
							return
						}
						if !ok {
							failed = true
						}
					}
				}
			}
			if !failed {
				c = true
				do = false
				if i.node.saveName == "" {
					b = d[KeyValueKey{Main: "ID"}]
				} else {
					notIncluded := make([]string, 0)
					i.g.DB.RLock()
					for k := range i.g.DB.OT[i.node.TypeName].Fields {
						_, ok = d[KeyValueKey{Main: k}]
						if !ok {
							notIncluded = append(notIncluded, k)
						}
					}
					i.g.DB.RUnlock()
					for _, v := range notIncluded {
						var buffer bytes.Buffer
						buffer.WriteString(i.g.DB.Options.DBName)
						buffer.WriteString(i.g.DB.KV.D)
						buffer.WriteString(i.node.TypeName)
						buffer.WriteString(i.g.DB.KV.D)
						buffer.Write(d[KeyValueKey{Main: "ID"}])
						buffer.WriteString(i.g.DB.KV.D)
						buffer.WriteString(v)

						item, err := i.txn.Get(buffer.Bytes())
						if err != nil && err != badger.ErrKeyNotFound {
							e = append(e, err)
						}
						if item != nil {
							val, err := item.ValueCopy(nil)
							if err != nil {
								e = append(e, err)
							} else {
								d[KeyValueKey{Main: v}] = val
							}
						}
					}
					a = d
				}
			}
		} else {
			e = append(e, errors.New("invalid result returned from first query"))
			return
		}
	}
	return
}

type iteratorLoaderGraphLink struct {
	node             *NodeQuery
	g                *GetterFactory
	txn              *badger.Txn
	prefix           string
	currentDirection string
	iterator         *badger.Iterator
	from             []byte
}

func (i *iteratorLoaderGraphLink) get2(from []byte) (a map[KeyValueKey][]byte, b LinkListList, c []error, loaded bool) {
	errs := make([]error, 0)
	i.from = from

	if i.node.Direction == "" {
		c = append(errs, errors.New("this nodequery does not have a direction specified"))
		return
	}

	switch i.node.Direction {
	case "->", "<-":
		if i.node.Direction == "->" {
			i.prefix = i.g.DB.Options.DBName + i.g.DB.KV.D + i.node.TypeName + i.g.DB.KV.D + "+" + i.g.DB.KV.D + string(from)
		} else {
			i.prefix = i.g.DB.Options.DBName + i.g.DB.KV.D + i.node.TypeName + i.g.DB.KV.D + "-" + i.g.DB.KV.D + string(from)
		}
		opt := badger.DefaultIteratorOptions
		opt.Prefix = []byte(i.prefix)
		opt.PrefetchSize = 20
		opt.PrefetchValues = false
		i.iterator = i.txn.NewIterator(opt)
		i.iterator.Seek([]byte(i.prefix))
		for !loaded {
			d := make(map[KeyValueKey][]byte)
			if !i.iterator.ValidForPrefix([]byte(i.prefix)) {
				loaded = false
				return
			}
			item := i.iterator.Item()
			key := item.KeyCopy(nil)
			ka := bytes.Split(key, []byte(i.g.DB.KV.D))
			failed := true
			if len(i.node.Instructions) < 1 {
				failed = false
			}
			for key, in1 := range i.node.Instructions {
				var fieldType FieldType
				var advancedFieldType AdvancedFieldType
				var va FieldOptions
				var ok bool
				i.g.DB.RLock()
				va, ok = i.g.DB.LT[i.node.TypeName].Fields[key]
				if !ok {
					c = append(errs, errors.New("field name -"+key+"- not found in database"))
					i.iterator.Close()
					i.g.DB.RUnlock()
					return
				}
				if !va.Advanced {
					fieldType = i.g.DB.FT[va.FieldType]
				} else {
					advancedFieldType = i.g.DB.AFT[va.FieldType]
				}
				i.g.DB.RUnlock()

				if !va.Advanced {
					var buffer bytes.Buffer
					buffer.WriteString(i.g.DB.Options.DBName)
					buffer.WriteString(i.g.DB.KV.D)
					buffer.WriteString(i.node.TypeName)
					buffer.WriteString(i.g.DB.KV.D)
					if i.node.Direction == "->" {
						buffer.Write(ka[3])
						buffer.WriteString(i.g.DB.KV.D)
						buffer.Write(ka[4])
					} else {
						buffer.Write(ka[4])
						buffer.WriteString(i.g.DB.KV.D)
						buffer.Write(ka[3])
					}
					buffer.WriteString(i.g.DB.KV.D)
					buffer.WriteString(key)
					item2, err := i.txn.Get(buffer.Bytes())
					if err != nil && err == badger.ErrKeyNotFound {
						failed = true
					} else if err != nil && err != badger.ErrKeyNotFound {
						Log.Error().Interface("error", err).Interface("stack", string(debug.Stack())).Msg("Getting value for key in Badger threw error again")
						c = append(errs, err)
						i.iterator.Close()
						return
					} else if err == nil && item2 != nil {
						var val []byte
						for _, in2 := range in1 {
							rawParam, err := fieldType.Set(in2.param)
							if err != nil {
								c = append(errs, err)
								return
							}
							val, err = item2.ValueCopy(nil)
							if err != nil {
								Log.Error().Interface("error", err).Str("key", string(item.KeyCopy(nil))).Interface("stack", string(debug.Stack())).Msg("Getting value for key after getting item in Badger threw error")
								i.iterator.Close()
								c = append(errs, err)
								return
							}
							ok, err := fieldType.Compare(in2.action, val, rawParam)
							if err != nil {
								c = append(errs, err)
								i.iterator.Close()
								return
							}
							if !ok {
								failed = true
								break
							}
						}
						if !failed {
							if i.node.saveName != "" {
								d[KeyValueKey{Main: key}] = val
							}
						}
					}
				} else {
					for _, in2 := range in1 {
						ok, err := advancedFieldType.Compare(i.txn, i.g.DB, false, i.node.TypeName, d[KeyValueKey{Main: "ID"}], d[KeyValueKey{Main: "ID"}], key, in2.action, in2.param)
						if len(err) >= 1 {
							c = append(errs, err...)
							return
						}
						if !ok {
							failed = true
						}
					}
				}

			}
			if !failed {

				loaded = true
				if i.node.saveName == "" {
					b = LinkListList{ka[3], ka[4]}
				} else {
					notIncluded := make([]string, 0)
					i.g.DB.RLock()
					for k := range i.g.DB.LT[i.node.TypeName].Fields {
						_, ok := d[KeyValueKey{Main: k}]
						if !ok {
							notIncluded = append(notIncluded, k)
						}
					}
					i.g.DB.RUnlock()
					for _, v := range notIncluded {
						var buffer bytes.Buffer
						buffer.WriteString(i.g.DB.Options.DBName)
						buffer.WriteString(i.g.DB.KV.D)
						buffer.WriteString(i.node.TypeName)
						buffer.WriteString(i.g.DB.KV.D)
						if i.node.Direction == "->" {
							buffer.Write(ka[3])
							buffer.WriteString(i.g.DB.KV.D)
							buffer.Write(ka[4])
						} else {
							buffer.Write(ka[4])
							buffer.WriteString(i.g.DB.KV.D)
							buffer.Write(ka[3])
						}
						buffer.WriteString(i.g.DB.KV.D)
						buffer.WriteString(v)
						item, err := i.txn.Get(buffer.Bytes())
						if err != nil && err != badger.ErrKeyNotFound {
							c = append(errs, err)
						}
						if item != nil {
							val, err := item.ValueCopy(nil)
							if err != nil {
								c = append(errs, err)
							} else {
								d[KeyValueKey{Main: v}] = val
							}
						}
					}
					d[KeyValueKey{Main: "FROM"}] = ka[3]
					d[KeyValueKey{Main: "TO"}] = ka[4]
					a = d
				}

			}
			i.iterator.Next()
		}
	case "-":
		i.g.DB.RLock()
		lt, ok := i.g.DB.LT[i.node.TypeName]
		i.g.DB.RUnlock()
		if !ok {
			c = append(errs, errors.New("link of type '"+i.node.TypeName+"' cannot be found in the database"))
			return
		}
		if lt.Type != 1 {
			c = append(errs, errors.New("link of type '"+i.node.TypeName+"' does not suppport direction -"))
			return
		}
		i.currentDirection = "->"
		i.prefix = i.g.DB.Options.DBName + i.g.DB.KV.D + i.node.TypeName + i.g.DB.KV.D + "+" + i.g.DB.KV.D + string(from)
		opt := badger.DefaultIteratorOptions
		opt.Prefix = []byte(i.prefix)
		opt.PrefetchSize = 20
		opt.PrefetchValues = false
		i.iterator = i.txn.NewIterator(opt)
		i.iterator.Seek([]byte(i.prefix))
		for !loaded {
			if !i.iterator.ValidForPrefix([]byte(i.prefix)) && i.currentDirection == "->" {
				i.currentDirection = "<-"
				i.prefix = i.g.DB.Options.DBName + i.g.DB.KV.D + i.node.TypeName + i.g.DB.KV.D + "-" + i.g.DB.KV.D + string(from)
				opt := badger.DefaultIteratorOptions
				opt.Prefix = []byte(i.prefix)
				opt.PrefetchSize = 20
				opt.PrefetchValues = false
				i.iterator = i.txn.NewIterator(opt)
				i.iterator.Seek([]byte(i.prefix))
			}
			if !i.iterator.ValidForPrefix([]byte(i.prefix)) && i.currentDirection == "<-" {
				loaded = false
				return
			}
			d := make(map[KeyValueKey][]byte)
			item := i.iterator.Item()
			key := item.KeyCopy(nil)
			ka := bytes.Split(key, []byte(i.g.DB.KV.D))
			failed := true
			if len(i.node.Instructions) < 1 {
				failed = false
			}
			for key, in1 := range i.node.Instructions {
				var fieldType FieldType
				var advancedFieldType AdvancedFieldType
				var va FieldOptions
				var ok bool
				i.g.DB.RLock()
				va, ok = i.g.DB.LT[i.node.TypeName].Fields[key]
				if !ok {
					c = append(errs, errors.New("field name -"+key+"- not found in database"))
					i.iterator.Close()
					i.g.DB.RUnlock()
					return
				}
				if !va.Advanced {
					fieldType = i.g.DB.FT[va.FieldType]
				} else {
					advancedFieldType = i.g.DB.AFT[va.FieldType]
				}
				i.g.DB.RUnlock()
				if !va.Advanced {
					var buffer bytes.Buffer
					buffer.WriteString(i.g.DB.Options.DBName)
					buffer.WriteString(i.g.DB.KV.D)
					buffer.WriteString(i.node.TypeName)
					buffer.WriteString(i.g.DB.KV.D)
					if i.currentDirection == "->" {
						buffer.Write(ka[3])
						buffer.WriteString(i.g.DB.KV.D)
						buffer.Write(ka[4])
					} else {
						buffer.Write(ka[4])
						buffer.WriteString(i.g.DB.KV.D)
						buffer.Write(ka[3])
					}
					buffer.WriteString(i.g.DB.KV.D)
					buffer.WriteString(key)
					item2, err := i.txn.Get(buffer.Bytes())
					if err != nil && err == badger.ErrKeyNotFound {
						failed = true
					} else if err != nil && err != badger.ErrKeyNotFound {
						Log.Error().Interface("error", err).Interface("stack", string(debug.Stack())).Msg("Getting value for key in Badger threw error again")
						c = append(errs, err)
						i.iterator.Close()
						return
					} else if err == nil && item2 != nil {
						var val []byte
						for _, in2 := range in1 {
							rawParam, err := fieldType.Set(in2.param)
							if err != nil {
								c = append(errs, err)
								return
							}
							val, err = item2.ValueCopy(nil)
							if err != nil {
								Log.Error().Interface("error", err).Str("key", string(item.KeyCopy(nil))).Interface("stack", string(debug.Stack())).Msg("Getting value for key after getting item in Badger threw error")
								i.iterator.Close()
								c = append(errs, err)
								return
							}
							ok, err := fieldType.Compare(in2.action, val, rawParam)
							if err != nil {
								c = append(errs, err)
								i.iterator.Close()
								return
							}
							if !ok {
								failed = true
								break
							}
						}
						if !failed {
							if i.node.saveName != "" {
								d[KeyValueKey{Main: key}] = val
							}
						}
					}
				} else {
					for _, in2 := range in1 {
						ok, err := advancedFieldType.Compare(i.txn, i.g.DB, false, i.node.TypeName, d[KeyValueKey{Main: "ID"}], d[KeyValueKey{Main: "ID"}], key, in2.action, in2.param)
						if len(err) >= 1 {
							c = append(errs, err...)
							return
						}
						if !ok {
							failed = true
						}
					}
				}
			}
			if !failed {
				loaded = true
				if i.node.saveName == "" {
					b = LinkListList{ka[3], ka[4]}
				} else {
					notIncluded := make([]string, 0)
					i.g.DB.RLock()
					for k := range i.g.DB.LT[i.node.TypeName].Fields {
						_, ok := d[KeyValueKey{Main: k}]
						if !ok {
							notIncluded = append(notIncluded, k)
						}
					}
					i.g.DB.RUnlock()
					for _, v := range notIncluded {
						var buffer bytes.Buffer
						buffer.WriteString(i.g.DB.Options.DBName)
						buffer.WriteString(i.g.DB.KV.D)
						buffer.WriteString(i.node.TypeName)
						buffer.WriteString(i.g.DB.KV.D)
						if i.currentDirection == "->" {
							buffer.Write(ka[3])
							buffer.WriteString(i.g.DB.KV.D)
							buffer.Write(ka[4])
						} else {
							buffer.Write(ka[4])
							buffer.WriteString(i.g.DB.KV.D)
							buffer.Write(ka[3])
						}
						buffer.WriteString(i.g.DB.KV.D)
						buffer.WriteString(v)
						item, err := i.txn.Get(buffer.Bytes())
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
					d[KeyValueKey{Main: "FROM"}] = ka[3]
					d[KeyValueKey{Main: "TO"}] = ka[4]
					a = d
				}

			}

			i.iterator.Next()
		}
	}
	return
}
func (i *iteratorLoaderGraphLink) more2() (a map[KeyValueKey][]byte, b LinkListList, errs []error, loaded bool) {
	errs = make([]error, 0)

	switch i.node.Direction {
	case "->", "<-":
		for !loaded {
			d := make(map[KeyValueKey][]byte)
			if !i.iterator.ValidForPrefix([]byte(i.prefix)) {
				loaded = false
				return
			}
			item := i.iterator.Item()
			key := item.KeyCopy(nil)
			ka := bytes.Split(key, []byte(i.g.DB.KV.D))
			failed := true
			if len(i.node.Instructions) < 1 {
				failed = false
			}
			for key, in1 := range i.node.Instructions {
				var fieldType FieldType
				var advancedFieldType AdvancedFieldType
				var va FieldOptions
				var ok bool
				i.g.DB.RLock()
				va, ok = i.g.DB.LT[i.node.TypeName].Fields[key]
				if !ok {
					errs = append(errs, errors.New("Field name -"+key+"- not found in database"))
					i.iterator.Close()
					i.g.DB.RUnlock()
					return
				}
				if !va.Advanced {
					fieldType = i.g.DB.FT[va.FieldType]
				} else {
					advancedFieldType = i.g.DB.AFT[va.FieldType]
				}
				if !va.Advanced {
					i.g.DB.RUnlock()
					var buffer bytes.Buffer
					buffer.WriteString(i.g.DB.Options.DBName)
					buffer.WriteString(i.g.DB.KV.D)
					buffer.WriteString(i.node.TypeName)
					buffer.WriteString(i.g.DB.KV.D)
					if i.node.Direction == "->" {
						buffer.Write(ka[3])
						buffer.WriteString(i.g.DB.KV.D)
						buffer.Write(ka[4])
					} else {
						buffer.Write(ka[4])
						buffer.WriteString(i.g.DB.KV.D)
						buffer.Write(ka[3])
					}
					buffer.WriteString(i.g.DB.KV.D)
					buffer.WriteString(key)
					item2, err := i.txn.Get(buffer.Bytes())
					if err != nil && err == badger.ErrKeyNotFound {
						failed = true
					} else if err != nil && err != badger.ErrKeyNotFound {
						Log.Error().Interface("error", err).Interface("stack", string(debug.Stack())).Msg("Getting value for key in Badger threw error again")
						errs = append(errs, err)
						i.iterator.Close()
						return
					} else if err == nil && item2 != nil {
						var val []byte
						for _, in2 := range in1 {
							rawParam, err := fieldType.Set(in2.param)
							if err != nil {
								errs = append(errs, err)
								return
							}
							val, err = item2.ValueCopy(nil)
							if err != nil {
								Log.Error().Interface("error", err).Str("key", string(item.KeyCopy(nil))).Interface("stack", string(debug.Stack())).Msg("Getting value for key after getting item in Badger threw error")
								i.iterator.Close()
								errs = append(errs, err)
								return
							}
							ok, err := fieldType.Compare(in2.action, val, rawParam)
							if err != nil {
								errs = append(errs, err)
								i.iterator.Close()
								return
							}
							if !ok {
								failed = true
								break
							}
						}
						if !failed {
							if i.node.saveName != "" {
								d[KeyValueKey{Main: key}] = val
							}
						}
					}
				} else {
					for _, in2 := range in1 {
						ok, err := advancedFieldType.Compare(i.txn, i.g.DB, false, i.node.TypeName, d[KeyValueKey{Main: "ID"}], d[KeyValueKey{Main: "ID"}], key, in2.action, in2.param)
						if len(err) >= 1 {
							errs = append(errs, err...)
							return
						}
						if !ok {
							failed = true
						}
					}
				}
			}
			if !failed {
				loaded = true
				if i.node.saveName == "" {
					b = LinkListList{ka[3], ka[4]}
				} else {
					notIncluded := make([]string, 0)
					i.g.DB.RLock()
					for k := range i.g.DB.LT[i.node.TypeName].Fields {
						_, ok := d[KeyValueKey{Main: k}]
						if !ok {
							notIncluded = append(notIncluded, k)
						}
					}
					i.g.DB.RUnlock()
					for _, v := range notIncluded {
						var buffer bytes.Buffer
						buffer.WriteString(i.g.DB.Options.DBName)
						buffer.WriteString(i.g.DB.KV.D)
						buffer.WriteString(i.node.TypeName)
						buffer.WriteString(i.g.DB.KV.D)
						if i.node.Direction == "->" {
							buffer.Write(ka[3])
							buffer.WriteString(i.g.DB.KV.D)
							buffer.Write(ka[4])
						} else {
							buffer.Write(ka[4])
							buffer.WriteString(i.g.DB.KV.D)
							buffer.Write(ka[3])
						}
						buffer.WriteString(i.g.DB.KV.D)
						buffer.WriteString(v)
						item, err := i.txn.Get(buffer.Bytes())
						if err != nil && err != badger.ErrKeyNotFound {
							errs = append(errs, err)
							loaded = false
							return
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
					d[KeyValueKey{Main: "FROM"}] = ka[3]
					d[KeyValueKey{Main: "TO"}] = ka[4]
					a = d
				}

			}

			i.iterator.Next()
		}
	case "-":
		//i.iterator.Next()
		for !loaded {
			if !i.iterator.ValidForPrefix([]byte(i.prefix)) && i.currentDirection == "->" {
				i.currentDirection = "<-"
				i.prefix = i.g.DB.Options.DBName + i.g.DB.KV.D + i.node.TypeName + i.g.DB.KV.D + "-" + i.g.DB.KV.D + string(i.from)
				opt := badger.DefaultIteratorOptions
				opt.Prefix = []byte(i.prefix)
				opt.PrefetchSize = 20
				opt.PrefetchValues = false
				i.iterator = i.txn.NewIterator(opt)
				i.iterator.Seek([]byte(i.prefix))
			}
			if !i.iterator.ValidForPrefix([]byte(i.prefix)) && i.currentDirection == "<-" {
				loaded = false
				return
			}
			d := make(map[KeyValueKey][]byte)
			item := i.iterator.Item()
			key := item.KeyCopy(nil)
			ka := bytes.Split(key, []byte(i.g.DB.KV.D))
			failed := true
			if len(i.node.Instructions) < 1 {
				failed = false
			}
			for key, in1 := range i.node.Instructions {
				var fieldType FieldType
				var advancedFieldType AdvancedFieldType
				var va FieldOptions
				var ok bool
				i.g.DB.RLock()
				va, ok = i.g.DB.LT[i.node.TypeName].Fields[key]
				if !ok {
					errs = append(errs, errors.New("Field name -"+key+"- not found in database"))
					i.iterator.Close()
					i.g.DB.RUnlock()
					return
				}
				if !va.Advanced {
					fieldType = i.g.DB.FT[va.FieldType]
				} else {
					advancedFieldType = i.g.DB.AFT[va.FieldType]
				}
				if !va.Advanced {
					i.g.DB.RUnlock()
					var buffer bytes.Buffer
					buffer.WriteString(i.g.DB.Options.DBName)
					buffer.WriteString(i.g.DB.KV.D)
					buffer.WriteString(i.node.TypeName)
					buffer.WriteString(i.g.DB.KV.D)
					if i.currentDirection == "->" {
						buffer.Write(ka[3])
						buffer.WriteString(i.g.DB.KV.D)
						buffer.Write(ka[4])
					} else {
						buffer.Write(ka[4])
						buffer.WriteString(i.g.DB.KV.D)
						buffer.Write(ka[3])
					}
					buffer.WriteString(i.g.DB.KV.D)
					buffer.WriteString(key)
					item2, err := i.txn.Get(buffer.Bytes())
					if err != nil && err == badger.ErrKeyNotFound {
						failed = true
					} else if err != nil && err != badger.ErrKeyNotFound {
						Log.Error().Interface("error", err).Interface("stack", string(debug.Stack())).Msg("Getting value for key in Badger threw error again")
						errs = append(errs, err)
						i.iterator.Close()
						return
					} else if err == nil && item2 != nil {
						var val []byte
						for _, in2 := range in1 {
							rawParam, err := fieldType.Set(in2.param)
							if err != nil {
								errs = append(errs, err)
								return
							}
							val, err = item2.ValueCopy(nil)
							if err != nil {
								Log.Error().Interface("error", err).Str("key", string(item.KeyCopy(nil))).Interface("stack", string(debug.Stack())).Msg("Getting value for key after getting item in Badger threw error")
								i.iterator.Close()
								errs = append(errs, err)
								return
							}
							ok, err := fieldType.Compare(in2.action, val, rawParam)
							if err != nil {
								errs = append(errs, err)
								i.iterator.Close()
								return
							}
							if !ok {
								failed = true
								break
							}
						}
						if !failed {
							if i.node.saveName != "" {
								d[KeyValueKey{Main: key}] = val
							}
						}
					}
				} else {
					for _, in2 := range in1 {
						ok, err := advancedFieldType.Compare(i.txn, i.g.DB, false, i.node.TypeName, d[KeyValueKey{Main: "ID"}], d[KeyValueKey{Main: "ID"}], key, in2.action, in2.param)
						if len(err) >= 1 {
							errs = append(errs, err...)
							return
						}
						if !ok {
							failed = true
						}
					}
				}
			}
			if !failed {
				loaded = true
				if i.node.saveName == "" {
					b = LinkListList{ka[3], ka[4]}
				} else {
					notIncluded := make([]string, 0)
					i.g.DB.RLock()
					for k := range i.g.DB.LT[i.node.TypeName].Fields {
						_, ok := d[KeyValueKey{Main: k}]
						if !ok {
							notIncluded = append(notIncluded, k)
						}
					}
					i.g.DB.RUnlock()
					for _, v := range notIncluded {
						var buffer bytes.Buffer
						buffer.WriteString(i.g.DB.Options.DBName)
						buffer.WriteString(i.g.DB.KV.D)
						buffer.WriteString(i.node.TypeName)
						buffer.WriteString(i.g.DB.KV.D)
						if i.currentDirection == "->" {
							buffer.Write(ka[3])
							buffer.WriteString(i.g.DB.KV.D)
							buffer.Write(ka[4])
						} else {
							buffer.Write(ka[4])
							buffer.WriteString(i.g.DB.KV.D)
							buffer.Write(ka[3])
						}
						buffer.WriteString(i.g.DB.KV.D)
						buffer.WriteString(v)
						item, err := i.txn.Get(buffer.Bytes())
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
					d[KeyValueKey{Main: "FROM"}] = ka[3]
					d[KeyValueKey{Main: "TO"}] = ka[4]
					a = d
				}

			}

			i.iterator.Next()
		}

	}
	return
}
func (i *iteratorLoaderGraphLink) close() {
	if i.iterator != nil {
		i.iterator.Close()
	}
}

type iteratorLoaderGraphObject struct {
	node     *NodeQuery
	g        *GetterFactory
	txn      *badger.Txn
	gottenID bool
}

func (i *iteratorLoaderGraphObject) get(to []byte) (a map[KeyValueKey][]byte, b []byte, errs []error, success bool) {
	if i.node.saveName != "" {
		a = make(map[KeyValueKey][]byte)
	}
	errs = make([]error, 0)
	ins, ok := i.node.Instructions["ID"]
	if ok {
		for _, query := range ins {
			if query.action == "==" {

				queryID, ok := query.param.(uint64)
				if !ok {
					errs = append(errs, errors.New("invadid ID provided in node query expected uint64 got"))
					return
				}
				i.g.DB.RLock()
				toGotten, err := i.g.DB.FT["uint64"].Get(to)
				i.g.DB.RUnlock()
				if err != nil {
					errs = append(errs, err)
					return
				}
				if toGotten != queryID {
					success = false
					return
				}
				success = true
				if i.node.saveName == "" {
					b = to
				} else {
					obj, err := i.g.getKeysWithValue(i.txn, i.g.DB.Options.DBName, i.node.TypeName, string(to))
					if len(err) > 0 {
						errs = append(errs, err...)
					}
					if obj != nil {
						obj[KeyValueKey{Main: "ID"}] = to
						a = obj
					}
				}
				return
			}

		}
	}

	failed := false
	for key, in1 := range i.node.Instructions {
		var fieldType FieldType
		var advancedFieldType AdvancedFieldType
		var va FieldOptions
		var ok bool
		i.g.DB.RLock()
		va, ok = i.g.DB.OT[i.node.TypeName].Fields[key]
		if !ok {
			errs = append(errs, errors.New("Field name -"+key+"- not found in database"))
			i.g.DB.RUnlock()
			return
		}
		if !va.Advanced {
			fieldType = i.g.DB.FT[va.FieldType]
		} else {
			advancedFieldType = i.g.DB.AFT[va.FieldType]
		}
		if !va.Advanced {
			i.g.DB.RUnlock()
			var buffer bytes.Buffer
			buffer.WriteString(i.g.DB.Options.DBName)
			buffer.WriteString(i.g.DB.KV.D)
			buffer.WriteString(i.node.TypeName)
			buffer.WriteString(i.g.DB.KV.D)
			buffer.Write(to)
			buffer.WriteString(i.g.DB.KV.D)
			buffer.WriteString(key)
			item2, err := i.txn.Get(buffer.Bytes())
			if err != nil && err == badger.ErrKeyNotFound {
				failed = true
			} else if err != nil && err != badger.ErrKeyNotFound {
				Log.Error().Interface("error", err).Interface("stack", string(debug.Stack())).Msg("Getting value for key in Badger threw error again")
				errs = append(errs, err)
				return
			} else if err == nil && item2 != nil {
				var val []byte
				for _, in2 := range in1 {
					rawParam, err := fieldType.Set(in2.param)
					if err != nil {
						errs = append(errs, err)
						return
					}
					val, err = item2.ValueCopy(nil)
					if err != nil {
						Log.Error().Interface("error", err).Str("key", string(item2.KeyCopy(nil))).Interface("stack", string(debug.Stack())).Msg("Getting value for key after getting item in Badger threw error")
						errs = append(errs, err)
						return
					}
					ok, err := fieldType.Compare(in2.action, val, rawParam)
					if err != nil {
						errs = append(errs, err)
						return
					}
					if !ok {
						failed = true
						break
					}
				}
				if !failed {
					if i.node.saveName != "" {
						a[KeyValueKey{Main: key}] = val
					}
				}
			}
		} else {
			for _, in2 := range in1 {
				ok, err := advancedFieldType.Compare(i.txn, i.g.DB, true, i.node.TypeName, to, to, key, in2.action, in2.param)
				if len(err) >= 1 {
					errs = append(errs, err...)
					return
				}
				if !ok {
					failed = true
				}
			}
		}

	}
	if !failed {
		success = true
		if i.node.saveName != "" {
			notIncluded := make([]string, 0)
			i.g.DB.RLock()
			for k := range i.g.DB.OT[i.node.TypeName].Fields {
				_, ok := a[KeyValueKey{Main: k}]
				if !ok {
					notIncluded = append(notIncluded, k)
				}
			}
			i.g.DB.RUnlock()
			for _, v := range notIncluded {
				var buffer bytes.Buffer
				buffer.WriteString(i.g.DB.Options.DBName)
				buffer.WriteString(i.g.DB.KV.D)
				buffer.WriteString(i.node.TypeName)
				buffer.WriteString(i.g.DB.KV.D)
				buffer.Write(to)
				buffer.WriteString(i.g.DB.KV.D)
				buffer.WriteString(v)
				item, err := i.txn.Get(buffer.Bytes())
				if err != nil && err != badger.ErrKeyNotFound {
					errs = append(errs, err)
				}
				if item != nil {
					val, err := item.ValueCopy(nil)
					if err != nil {
						errs = append(errs, err)
					} else {
						a[KeyValueKey{Main: v}] = val
					}
				}
			}

			a[KeyValueKey{Main: "ID"}] = to
		} else {
			b = to
		}

	} else {
		success = false
	}

	return
}

type holder struct {
	query              *NodeQuery
	first              *iteratorLoaderGraphStart
	link               *iteratorLoaderGraphLink
	object             *iteratorLoaderGraphObject
	dataObjectFirst    []map[KeyValueKey][]byte
	dataObject         map[string]map[KeyValueKey][]byte
	idsObject          map[string]struct{}
	idsLink            map[string]LinkListList
	currentObject      map[KeyValueKey][]byte
	currentIDObject    []byte
	currentIDLink      LinkListList
	loadedKeys         []string
	sentCurrentToArray bool
	skiped             int
	count              int
}

//GetterGraphPartern ..
//action: 'objects'
//params... NodeQuery (NodeQuery)
//placesses objectLists found in  node query [0] in variable [1]
func GetterGraphPartern(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	hold := make([]holder, len(qData))
	defer func() {
		for i := range hold {
			if hold[i].first != nil {
				hold[i].first.close()
			}
			if hold[i].link != nil {
				hold[i].link.close()
			}
		}
	}()

	for i, v := range qData {
		n, ok := v.(NodeQuery)
		if !ok {
			ret.Errors = append(ret.Errors, errors.New("invalid argument provided in graph.p expected Nodequery"))
		}
		h := holder{}
		h.query = &n
		if i == (len(qData) - 1) {
			if h.query.limit == 0 {
				h.query.limit = 100
			}
		}
		j := i + 1
		if j == 1 {
			s := iteratorLoaderGraphStart{}
			s.setup(g, h.query, txn)
			h.first = &s
			if h.query.saveName != "" {
				m := make(map[string]map[KeyValueKey][]byte)
				h.dataObject = m
			} else {
				m := make(map[string]struct{})
				h.idsObject = m
			}
		}
		if j%2 == 0 {
			s := iteratorLoaderGraphLink{node: h.query, g: g, txn: txn}
			h.link = &s
			if h.query.saveName != "" {
				m := make(map[string]map[KeyValueKey][]byte)
				h.dataObject = m
			} else {
				m := make(map[string]LinkListList)
				h.idsLink = m
			}
		}
		if j%2 != 0 && i != 1 {
			s := iteratorLoaderGraphObject{h.query, g, txn, false}
			h.object = &s
			if h.query.saveName != "" {
				m := make(map[string]map[KeyValueKey][]byte)
				h.dataObject = m
			} else {
				m := make(map[string]struct{})
				h.idsObject = m
			}
		}

		hold[i] = h
	}

	do := true
	position := 0
	down := true

	for do {
		// deal with the first node
		if position == 0 {
			//execution came up so change current and if successfull go back down if not call it a day
			//Log.Print("-------------------------------- " + strconv.Itoa(position+1))
			obj, byt, loaded, errs := hold[position].first.next2()
			if len(errs) > 0 {
				ret.Errors = append(ret.Errors, errs...)
				return
			}
			if loaded {
				if hold[position].first.node.saveName == "" {
					hold[position].currentIDObject = byt
				} else {
					hold[position].currentObject = obj
				}
				hold[position].sentCurrentToArray = false
				position = position + 1
				down = true
			} else {
				do = false // just quit because without a new first object the query is as good as done
				//Log.Print("--------------------------Node Start (Stop Execution)")
			}
		}

		if ((position + 1) % 2) == 0 { // if it is a link

			if down {
				//Log.Print("----------------------------- " + strconv.Itoa(position+1))
				var prevCur []byte
				prevPosition := position - 1

				if hold[position].query.Direction != "-" {
					g.DB.RLock()
					fromName := hold[prevPosition].query.TypeName
					lt := g.DB.LT[hold[position].query.TypeName]
					toName := hold[position+1].query.TypeName
					g.DB.RUnlock()
					if hold[position].query.Direction == "->" {
						if fromName != lt.From {
							ret.Errors = append(ret.Errors, errors.New("Link of type "+hold[position].query.TypeName+" with direction -> does not support this 'FROM' -'"+fromName+"'-"))
							return
						}
						if toName != lt.To {
							ret.Errors = append(ret.Errors, errors.New("Link of type "+hold[position].query.TypeName+" with direction -> does not support this 'TO' -'"+toName+"'-"))
							return
						}
					} else if hold[position].query.Direction == "<-" {
						if fromName != lt.To {
							ret.Errors = append(ret.Errors, errors.New("Link of type "+hold[position].query.TypeName+" with direction <- does not support this 'FROM' -'"+fromName+"'-"))
							return
						}
						if toName != lt.From {
							ret.Errors = append(ret.Errors, errors.New("Link of type "+hold[position].query.TypeName+" with direction <- does not support this 'TO' -'"+toName+"'-"))
							return
						}
					}
				}

				if hold[prevPosition].query.saveName == "" {
					prevCur = hold[prevPosition].currentIDObject
				} else {
					prevCur = hold[prevPosition].currentObject[KeyValueKey{Main: "ID"}]
				}
				if prevCur == nil {
					ret.Errors = append(ret.Errors, errors.New("Invalid previous id provided from nodequery "+fmt.Sprint(prevPosition)+", ID: "+string(prevCur)))
					return
				}
				obj, link, errs, loaded := hold[position].link.get2(prevCur)
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
					return
				}
				if loaded {
					if hold[position].query.saveName == "" {
						hold[position].currentIDLink = link

						//f, _ := binary.Uvarint(link.FROM)
						//t, _ := binary.Uvarint(link.TO)
						//Log.Print(strconv.Itoa(int(f)) + " ____ " + hold[position].link.currentDirection + " ____ " + strconv.Itoa(int(t)))
					} else {
						hold[position].currentObject = obj

						//f, _ := binary.Uvarint(obj[KeyValueKey{Main: "FROM"}])
						//t, _ := binary.Uvarint(obj[KeyValueKey{Main: "TO"}])
						//Log.Print(strconv.Itoa(int(f)) + " ____ " + hold[position].link.currentDirection + " ____ " + strconv.Itoa(int(t)))
					}
					hold[position].sentCurrentToArray = false
					position = position + 1
					down = true
				} else {
					// Unable to get even one object so go to the previous node to change prevCur
					position = position - 1
					down = false
				}
			} else {
				//Log.Print("----------------------------- " + strconv.Itoa(position+1))
				obj, link, errs, loaded := hold[position].link.more2()
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
					return
				}
				if loaded {
					if hold[position].query.saveName == "" {
						hold[position].currentIDLink = link

						//f, _ := binary.Uvarint(link.FROM)
						//t, _ := binary.Uvarint(link.TO)
						//Log.Print(strconv.Itoa(int(f)) + " ____ " + hold[position].link.currentDirection + " ____ " + strconv.Itoa(int(t)))
					} else {
						hold[position].currentObject = obj

						//f, _ := binary.Uvarint(obj[KeyValueKey{Main: "FROM"}])
						//t, _ := binary.Uvarint(obj[KeyValueKey{Main: "TO"}])
						//Log.Print(strconv.Itoa(int(f)) + " ____ " + hold[position].link.currentDirection + " ____ " + strconv.Itoa(int(t)))
					}
					hold[position].sentCurrentToArray = false
					position = position + 1
					down = true
				} else {
					// Unable to get even one object so go to the previous node to change prevCur
					position = position - 1
					down = false
				}
			}
		}

		if ((position+1)%2) != 0 && position != 0 && position != (len(hold)-1) { // if it an object query that is not the first and the last
			if down {
				//Log.Print("----------------------------- " + strconv.Itoa(position+1))
				var prevCur []byte
				prevPosition := position - 1
				if hold[prevPosition].query.saveName == "" {
					prevCur = hold[prevPosition].currentIDLink.TO
				} else {
					prevCur = hold[prevPosition].currentObject[KeyValueKey{Main: "TO"}]
				}
				if prevCur == nil {
					ret.Errors = append(ret.Errors, errors.New("Invalid previous id provided from nodequery "+fmt.Sprint(prevPosition)+", ID: "+string(prevCur)))
					return
				}
				obj, byt, errs, loaded := hold[position].object.get(prevCur)
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
					return
				}
				if loaded {
					if hold[position].query.saveName == "" {
						hold[position].currentIDObject = byt
					} else {
						hold[position].currentObject = obj
					}

					hold[position].sentCurrentToArray = false
					position = position + 1
					down = true
				} else {
					// Unable to get even one object so go to the previous node to change prevCur
					position = position - 1
					down = false
				}
			} else { // there is no point waiting here if the current is no longer valid we go up to get a new one
				//Log.Print("----------------------------- " + strconv.Itoa(position+1))
				position = position - 1
				down = false
			}
		}

		if position == (len(hold) - 1) { // if the last query, that is definitely an object query
			//Log.Print("----------------------------- " + strconv.Itoa(position+1))
			var prevCur []byte
			prevPosition := position - 1
			if hold[prevPosition].query.saveName == "" {
				prevCur = hold[prevPosition].currentIDLink.TO
			} else {
				prevCur = hold[prevPosition].currentObject[KeyValueKey{Main: "TO"}]
			}
			if prevCur == nil {
				ret.Errors = append(ret.Errors, errors.New("Invalid previous id provided from nodequery "+fmt.Sprint(prevPosition)+", ID: "+string(prevCur)))
				return
			}
			obj, byt, errs, loaded := hold[position].object.get(prevCur)
			if len(errs) > 0 {
				ret.Errors = append(ret.Errors, errs...)
				return
			}
			if loaded {
				if hold[position].query.saveName == "" {
					if !bytes.Equal(byt, hold[position].currentIDObject) {
						hold[position].sentCurrentToArray = false
					}
					hold[position].currentIDObject = byt
				} else {
					if !bytes.Equal(obj[KeyValueKey{Main: "ID"}], hold[position].currentObject[KeyValueKey{Main: "ID"}]) {
						hold[position].sentCurrentToArray = false
					}
					hold[position].currentObject = obj
				}

				for i := range hold {
					if !hold[i].sentCurrentToArray {
						if hold[i].query.Direction == "" && hold[i].query.saveName != "" { // it is an object query and saved
							if hold[i].query.skip > hold[i].skiped {
								hold[i].skiped = hold[i].skiped + 1
							} else {
								hold[i].dataObject[string(hold[i].currentObject[KeyValueKey{Main: "ID"}])] = hold[i].currentObject
								hold[i].sentCurrentToArray = true
								hold[i].count = hold[i].count + 1
							}
							if hold[i].count >= hold[i].query.limit {
								do = false
							}
						} else if hold[i].query.Direction != "" && hold[i].query.saveName != "" { // it is a link query and saved
							if hold[i].query.skip > hold[i].skiped {
								hold[i].skiped = hold[i].skiped + 1
							} else {
								hold[i].dataObject[string(hold[i].currentObject[KeyValueKey{Main: "FROM"}])+string(hold[i].currentObject[KeyValueKey{Main: "TO"}])] = hold[i].currentObject
								hold[i].sentCurrentToArray = true
								hold[i].count = hold[i].count + 1
							}
							if hold[i].count >= hold[i].query.limit {
								do = false
							}
						} else if hold[i].query.Direction == "" && hold[i].query.saveName == "" { // it is an object query and not saved
							//ele.idsObject[string(ele.currentIDObject)] = ele.currentIDObject
							hold[i].sentCurrentToArray = true
						} else if hold[i].query.Direction != "" && hold[i].query.saveName == "" { // it is a link query and not saved
							//*ele.idsLink = append(*ele.idsLink, ele.currentIDLink)
							hold[i].sentCurrentToArray = true
						}
					}
				}

				position = position - 1
				down = false

			} else {
				position = position - 1
				down = false
			}
		}
	}

	for _, v := range hold {
		if v.query.saveName != "" {
			d := make([]map[KeyValueKey][]byte, 0)
			for _, vv := range v.dataObject {
				d = append(d, vv)
			}
			if v.query.Direction != "" { //if it is a link
				l := LinkList{}
				l.LinkName = v.query.TypeName
				l.isIds = false
				l.Links = d
				l.order.field = v.query.Sort
				l.order.typ = v.query.SortType
				l.fields = v.query.Fields
				(*data)[v.query.saveName] = &l
			} else {
				o := ObjectList{}
				o.ObjectName = v.query.TypeName
				o.isIds = false
				o.Objects = d
				o.order.field = v.query.Sort
				o.order.typ = v.query.SortType
				o.fields = v.query.Fields
				(*data)[v.query.saveName] = &o
			}
		}
	}

}

//GetterGraphPartern ..
//action: 'objects'
//params... NodeQuery (NodeQuery)
//placesses objectLists found in  node query [0] in variable [1]
func GetterGraphParternObjectStart(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	hold := make([]holder, len(qData))
	defer func() {
		for i := range hold {
			if hold[i].link != nil {
				hold[i].link.close()
			}
		}
	}()

	for i, v := range qData {
		var n NodeQuery
		var ok bool
		var h holder
		if i != 0 {
			n, ok = v.(NodeQuery)
			if !ok {
				ret.Errors = append(ret.Errors, errors.New("invalid argument provided in graph.p expected Nodequery"))
			}
			h = holder{}
			h.query = &n
			if i == (len(qData) - 1) {
				if h.query.limit == 0 {
					h.query.limit = 100
				}
			}
		} else {
			h = holder{}
		}

		j := i + 1
		if j == 1 {
			ke, ok := v.(string)
			if !ok {
				ret.Errors = append(ret.Errors, errors.New("invalid first argument provided in graph.p.o expected string"))
				return
			}
			var objectList *ObjectList
			objectList, ok = (*data)[ke].(*ObjectList)
			if !ok {
				ret.Errors = append(ret.Errors, errors.New("invalid data for first argument provided in graph.p.o expecting *objectlist"))
				return
			}
			objs := make([]map[KeyValueKey][]byte, 0)
			objs = append(objs, objectList.Objects...)
			h.dataObjectFirst = objs
			n := NodeQuery{}
			n.TypeName = objectList.ObjectName
			n.saveName = ke + "new"
			n.limit = len(objectList.Objects) + 1
			n.Sort = objectList.order.field
			n.SortType = objectList.order.typ
			n.Fields = objectList.fields
			h.query = &n
		}
		if j%2 == 0 {
			s := iteratorLoaderGraphLink{node: h.query, g: g, txn: txn}
			h.link = &s
			if h.query.saveName != "" {
				m := make(map[string]map[KeyValueKey][]byte)
				h.dataObject = m
			} else {
				m := make(map[string]LinkListList)
				h.idsLink = m
			}
		}
		if j%2 != 0 && i != 1 {
			s := iteratorLoaderGraphObject{h.query, g, txn, false}
			h.object = &s
			if h.query.saveName != "" {
				m := make(map[string]map[KeyValueKey][]byte)
				h.dataObject = m
			} else {
				m := make(map[string]struct{})
				h.idsObject = m
			}
		}

		hold[i] = h
	}

	do := true
	position := 0
	down := true

	for do {
		// deal with the first node
		if position == 0 {
			//execution came up so change current and if successfull go back down if not call it a day
			//Log.Print("-------------------------------- " + strconv.Itoa(position+1))
			if len(hold[position].dataObjectFirst) == 0 {
				do = false
			} else {
				object := hold[position].dataObjectFirst[0]
				hold[position].dataObjectFirst = hold[position].dataObjectFirst[1:]
				hold[position].currentObject = object
				hold[position].sentCurrentToArray = false
				position = position + 1
				down = true

			}
		}

		if ((position + 1) % 2) == 0 { // if it is a link

			if down {
				//Log.Print("----------------------------- " + strconv.Itoa(position+1))
				var prevCur []byte
				prevPosition := position - 1

				if hold[position].query.Direction != "-" {
					g.DB.RLock()
					fromName := hold[prevPosition].query.TypeName
					lt := g.DB.LT[hold[position].query.TypeName]
					toName := hold[position+1].query.TypeName
					g.DB.RUnlock()
					if hold[position].query.Direction == "->" {
						if fromName != lt.From {
							ret.Errors = append(ret.Errors, errors.New("Link of type "+hold[position].query.TypeName+" with direction -> does not support this 'FROM' -'"+fromName+"'-"))
							return
						}
						if toName != lt.To {
							ret.Errors = append(ret.Errors, errors.New("Link of type "+hold[position].query.TypeName+" with direction -> does not support this 'TO' -'"+toName+"'-"))
							return
						}
					} else if hold[position].query.Direction == "<-" {
						if fromName != lt.To {
							ret.Errors = append(ret.Errors, errors.New("Link of type "+hold[position].query.TypeName+" with direction <- does not support this 'FROM' -'"+fromName+"'-"))
							return
						}
						if toName != lt.From {
							ret.Errors = append(ret.Errors, errors.New("Link of type "+hold[position].query.TypeName+" with direction <- does not support this 'TO' -'"+toName+"'-"))
							return
						}
					}
				}

				if hold[prevPosition].query.saveName == "" {
					prevCur = hold[prevPosition].currentIDObject
				} else {
					prevCur = hold[prevPosition].currentObject[KeyValueKey{Main: "ID"}]
				}
				if prevCur == nil {
					ret.Errors = append(ret.Errors, errors.New("Invalid previous id provided from nodequery "+fmt.Sprint(prevPosition)+", ID: "+string(prevCur)))
					return
				}
				obj, link, errs, loaded := hold[position].link.get2(prevCur)
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
					return
				}
				if loaded {
					if hold[position].query.saveName == "" {
						hold[position].currentIDLink = link

						//f, _ := binary.Uvarint(link.FROM)
						//t, _ := binary.Uvarint(link.TO)
						//Log.Print(strconv.Itoa(int(f)) + " ____ " + hold[position].link.currentDirection + " ____ " + strconv.Itoa(int(t)))
					} else {
						hold[position].currentObject = obj

						//f, _ := binary.Uvarint(obj[KeyValueKey{Main: "FROM"}])
						//t, _ := binary.Uvarint(obj[KeyValueKey{Main: "TO"}])
						//Log.Print(strconv.Itoa(int(f)) + " ____ " + hold[position].link.currentDirection + " ____ " + strconv.Itoa(int(t)))
					}
					hold[position].sentCurrentToArray = false
					position = position + 1
					down = true
				} else {
					// Unable to get even one object so go to the previous node to change prevCur
					position = position - 1
					down = false
				}
			} else {
				//Log.Print("----------------------------- " + strconv.Itoa(position+1))
				obj, link, errs, loaded := hold[position].link.more2()
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
					return
				}
				if loaded {
					if hold[position].query.saveName == "" {
						hold[position].currentIDLink = link

						//f, _ := binary.Uvarint(link.FROM)
						//t, _ := binary.Uvarint(link.TO)
						//Log.Print(strconv.Itoa(int(f)) + " ____ " + hold[position].link.currentDirection + " ____ " + strconv.Itoa(int(t)))
					} else {
						hold[position].currentObject = obj

						//f, _ := binary.Uvarint(obj[KeyValueKey{Main: "FROM"}])
						//t, _ := binary.Uvarint(obj[KeyValueKey{Main: "TO"}])
						//Log.Print(strconv.Itoa(int(f)) + " ____ " + hold[position].link.currentDirection + " ____ " + strconv.Itoa(int(t)))
					}
					hold[position].sentCurrentToArray = false
					position = position + 1
					down = true
				} else {
					// Unable to get even one object so go to the previous node to change prevCur
					position = position - 1
					down = false
				}
			}
		}

		if ((position+1)%2) != 0 && position != 0 && position != (len(hold)-1) { // if it an object query that is not the first and the last
			if down {
				//Log.Print("----------------------------- " + strconv.Itoa(position+1))
				var prevCur []byte
				prevPosition := position - 1
				if hold[prevPosition].query.saveName == "" {
					prevCur = hold[prevPosition].currentIDLink.TO
				} else {
					prevCur = hold[prevPosition].currentObject[KeyValueKey{Main: "TO"}]
				}
				if prevCur == nil {
					ret.Errors = append(ret.Errors, errors.New("Invalid previous id provided from nodequery "+fmt.Sprint(prevPosition)+", ID: "+string(prevCur)))
					return
				}
				obj, byt, errs, loaded := hold[position].object.get(prevCur)
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
					return
				}
				if loaded {
					if hold[position].query.saveName == "" {
						hold[position].currentIDObject = byt
					} else {
						hold[position].currentObject = obj
					}

					hold[position].sentCurrentToArray = false
					position = position + 1
					down = true
				} else {
					// Unable to get even one object so go to the previous node to change prevCur
					position = position - 1
					down = false
				}
			} else { // there is no point waiting here if the current is no longer valid we go up to get a new one
				//Log.Print("----------------------------- " + strconv.Itoa(position+1))
				position = position - 1
				down = false
			}
		}

		if position == (len(hold) - 1) { // if the last query, that is definitely an object query
			//Log.Print("----------------------------- " + strconv.Itoa(position+1))
			var prevCur []byte
			prevPosition := position - 1
			if hold[prevPosition].query.saveName == "" {
				prevCur = hold[prevPosition].currentIDLink.TO
			} else {
				prevCur = hold[prevPosition].currentObject[KeyValueKey{Main: "TO"}]
			}
			if prevCur == nil {
				ret.Errors = append(ret.Errors, errors.New("Invalid previous id provided from nodequery "+fmt.Sprint(prevPosition)+", ID: "+string(prevCur)))
				return
			}
			obj, byt, errs, loaded := hold[position].object.get(prevCur)
			if len(errs) > 0 {
				ret.Errors = append(ret.Errors, errs...)
				return
			}
			if loaded {
				if hold[position].query.saveName == "" {
					if !bytes.Equal(byt, hold[position].currentIDObject) { //bytes.Compare(byt, hold[position].currentIDObject) != 0 {
						hold[position].sentCurrentToArray = false
					}
					hold[position].currentIDObject = byt
				} else {
					if !bytes.Equal(obj[KeyValueKey{Main: "ID"}], hold[position].currentObject[KeyValueKey{Main: "ID"}]) {
						hold[position].sentCurrentToArray = false
					}
					hold[position].currentObject = obj
				}

				for i := range hold {
					if !hold[i].sentCurrentToArray {
						if hold[i].query.Direction == "" && hold[i].query.saveName != "" { // it is an object query and saved
							if hold[i].query.skip > hold[i].skiped {
								hold[i].skiped++
							} else {
								hold[i].dataObject[string(hold[i].currentObject[KeyValueKey{Main: "ID"}])] = hold[i].currentObject
								hold[i].sentCurrentToArray = true
								hold[i].count++
							}
							if hold[i].count >= hold[i].query.limit {
								do = false
							}
						} else if hold[i].query.Direction != "" && hold[i].query.saveName != "" { // it is a link query and saved
							if hold[i].query.skip > hold[i].skiped {
								hold[i].skiped++
							} else {
								hold[i].dataObject[string(hold[i].currentObject[KeyValueKey{Main: "FROM"}])+string(hold[i].currentObject[KeyValueKey{Main: "TO"}])] = hold[i].currentObject
								hold[i].sentCurrentToArray = true
								hold[i].count++
							}
							if hold[i].count >= hold[i].query.limit {
								do = false
							}
						} else if hold[i].query.Direction == "" && hold[i].query.saveName == "" { // it is an object query and not saved
							//ele.idsObject[string(ele.currentIDObject)] = ele.currentIDObject
							hold[i].sentCurrentToArray = true
						} else if hold[i].query.Direction != "" && hold[i].query.saveName == "" { // it is a link query and not saved
							//*ele.idsLink = append(*ele.idsLink, ele.currentIDLink)
							hold[i].sentCurrentToArray = true
						}
					}
				}

				position = position - 1
				down = false

			} else {
				position = position - 1
				down = false
			}
		}
	}

	for _, v := range hold {
		if v.query.saveName != "" {
			d := make([]map[KeyValueKey][]byte, 0)
			for _, vv := range v.dataObject {
				d = append(d, vv)
			}
			if v.query.Direction != "" { //if it is a link
				l := LinkList{}
				l.LinkName = v.query.TypeName
				l.isIds = false
				l.Links = d
				l.order.field = v.query.Sort
				l.order.typ = v.query.SortType
				l.fields = v.query.Fields
				(*data)[v.query.saveName] = &l
			} else {
				o := ObjectList{}
				o.ObjectName = v.query.TypeName
				o.isIds = false
				o.Objects = d
				o.order.field = v.query.Sort
				o.order.typ = v.query.SortType
				o.fields = v.query.Fields
				(*data)[v.query.saveName] = &o
			}
		}
	}

}

//GetterGraphStraight ..
//action: 'objects'
//params... NodeQuery (NodeQuery)
//placesses objectLists found in  node query [0] in variable [1]
func GetterGraphStraight(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	hold := make([]holder, len(qData))
	defer func() {
		for i := range hold {

			if hold[i].first != nil {
				hold[i].first.close()
			}
			if hold[i].link != nil {
				hold[i].link.close()
			}
		}
	}()

	for i, v := range qData {
		n, ok := v.(NodeQuery)
		if !ok {
			ret.Errors = append(ret.Errors, errors.New("invalid argument provided in graph.p expected Nodequery"))
		}
		h := holder{}
		h.query = &n
		if i == (len(qData) - 1) {
			if h.query.limit == 0 {
				h.query.limit = 100
			}
		}
		j := i + 1
		if j == 1 {
			s := iteratorLoaderGraphStart{}
			s.setup(g, h.query, txn)
			h.first = &s
			if h.query.saveName != "" {
				m := make(map[string]map[KeyValueKey][]byte)
				h.dataObject = m
			} else {
				m := make(map[string]struct{})
				h.idsObject = m
			}
		}
		if j%2 == 0 {
			s := iteratorLoaderGraphLink{node: h.query, g: g, txn: txn}
			h.link = &s
			if h.query.saveName != "" {
				m := make(map[string]map[KeyValueKey][]byte)
				h.dataObject = m
			} else {
				m := make(map[string]LinkListList)
				h.idsLink = m
			}
		}
		if j%2 != 0 && i != 1 {
			s := iteratorLoaderGraphObject{h.query, g, txn, false}
			h.object = &s
			if h.query.saveName != "" {
				m := make(map[string]map[KeyValueKey][]byte)
				h.dataObject = m
			} else {
				m := make(map[string]struct{})
				h.idsObject = m
			}
		}

		hold[i] = h
	}

	do := true
	position := 1
	down := true
	last := 0

	a := 0
	b := 1
	c := 2

	for do {
		// deal with the first node
		if position == 1 {

			if a == 0 {
				//execution came up so change current and if successfull go back down if not call it a day
				//Log.Print("---------------------------- " + strconv.Itoa(a+1))
				obj, byt, loaded, errs := hold[a].first.next2()
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
					return
				}
				if loaded {
					if hold[a].query.saveName == "" {
						hold[a].currentIDObject = byt
					} else {
						hold[a].currentObject = obj
					}
					hold[a].sentCurrentToArray = false
					position = 2
					down = true
				} else {
					position = -1
					if hold[a].query.saveName == "" {
						if len(hold[a].idsObject) < 1 {
							do = false
							//Log.Print("--------------------------Node Start (Stop Execution)")
						}
					} else {
						if len(hold[a].dataObject) < 1 {
							do = false
							//Log.Print("--------------------------Node Start (Stop Execution)")
						}
					}
				}
			}

			if a != 0 { // if it an object query that is not the first and the last
				//Log.Print("----------------------------- " + strconv.Itoa(a+1))
				var key string
				if len(hold[a].loadedKeys) > 0 {
					key, hold[a].loadedKeys = hold[a].loadedKeys[len(hold[a].loadedKeys)-1], hold[a].loadedKeys[:len(hold[a].loadedKeys)-1]
				}

				if key == "" {
					position = -1
				} else {
					if hold[a].query.saveName == "" {
						hold[a].currentIDObject = []byte(key)
					} else {
						hold[a].currentObject = hold[a].dataObject[key]
					}
					hold[position].sentCurrentToArray = false
					position = 2
					down = true
				}
			}
		}

		if position == 2 { // if it is a link

			if down {
				//Log.Print("----------------------------- " + strconv.Itoa(b+1))
				var prevCur []byte

				if hold[b].query.Direction != "-" {
					g.DB.RLock()
					fromName := hold[a].query.TypeName
					lt, _ := g.DB.LT[hold[b].query.TypeName]
					toName := hold[c].query.TypeName
					g.DB.RUnlock()
					if hold[b].query.Direction == "->" {
						if fromName != lt.From {
							ret.Errors = append(ret.Errors, errors.New("Link of type "+hold[b].query.TypeName+" with direction -> does not support this 'FROM' -'"+fromName+"'-"))
							return
						}
						if toName != lt.To {
							ret.Errors = append(ret.Errors, errors.New("Link of type "+hold[b].query.TypeName+" with direction -> does not support this 'TO' -'"+toName+"'-"))
							return
						}
					} else if hold[b].query.Direction == "<-" {
						if fromName != lt.To {
							ret.Errors = append(ret.Errors, errors.New("Link of type "+hold[b].query.TypeName+" with direction <- does not support this 'FROM' -'"+fromName+"'-"))
							return
						}
						if toName != lt.From {
							ret.Errors = append(ret.Errors, errors.New("Link of type "+hold[b].query.TypeName+" with direction <- does not support this 'TO' -'"+toName+"'-"))
							return
						}
					}
				}

				if hold[a].query.saveName == "" {
					prevCur = hold[a].currentIDObject
				} else {
					prevCur = hold[a].currentObject[KeyValueKey{Main: "ID"}]
				}
				if prevCur == nil {
					ret.Errors = append(ret.Errors, errors.New("Invalid previous id provided from nodequery "+fmt.Sprint(a)+", ID: "+string(prevCur)))
					return
				}
				obj, link, errs, loaded := hold[b].link.get2(prevCur)
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
					return
				}
				if loaded {
					if hold[b].query.saveName == "" {
						hold[b].currentIDLink = link
						//f, _ := binary.Uvarint(link.FROM)
						//t, _ := binary.Uvarint(link.TO)
						//Log.Print(strconv.Itoa(int(f)) + " ____ " + hold[b].link.currentDirection + " ____ " + strconv.Itoa(int(t)))
					} else {
						hold[b].currentObject = obj
						//f, _ := binary.Uvarint(obj[KeyValueKey{Main: "FROM"}])
						//t, _ := binary.Uvarint(obj[KeyValueKey{Main: "TO"}])
						//Log.Print(strconv.Itoa(int(f)) + " ____ " + hold[b].link.currentDirection + " ____ " + strconv.Itoa(int(t)))
					}
					hold[b].sentCurrentToArray = false
					position = 3
					down = true
				} else {
					// Unable to get even one object so go to the previous node to change prevCur
					position = 1
				}
			} else {
				//Log.Print("----------------------------- " + strconv.Itoa(b+1))
				obj, link, errs, loaded := hold[b].link.more2()
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
					return
				}
				if loaded {
					if hold[b].query.saveName == "" {
						hold[b].currentIDLink = link

						//f, _ := binary.Uvarint(link.FROM)
						//t, _ := binary.Uvarint(link.TO)
						//Log.Print(strconv.Itoa(int(f)) + " ____ " + hold[b].link.currentDirection + " ____ " + strconv.Itoa(int(t)))
					} else {
						hold[b].currentObject = obj

						//f, _ := binary.Uvarint(obj[KeyValueKey{Main: "FROM"}])
						//t, _ := binary.Uvarint(obj[KeyValueKey{Main: "TO"}])
						//Log.Print(strconv.Itoa(int(f)) + " ____ " + hold[b].link.currentDirection + " ____ " + strconv.Itoa(int(t)))
					}
					hold[b].sentCurrentToArray = false
					position = 3
					down = true
				} else {
					// Unable to get even one object so go to the previous node to change prevCur
					position = 1
				}
			}
		}

		if position == 3 { // if the last query, that is definitely an object query
			//Log.Print("----------------------------- " + strconv.Itoa(c+1))
			last++
			var prevCur []byte
			if hold[b].query.saveName == "" {
				prevCur = hold[b].currentIDLink.TO
			} else {
				prevCur = hold[b].currentObject[KeyValueKey{Main: "TO"}]
			}
			if prevCur == nil {
				ret.Errors = append(ret.Errors, errors.New("Invalid previous id provided from nodequery "+fmt.Sprint(b)+", ID: "+string(prevCur)))
				return
			}
			obj, byt, errs, loaded := hold[c].object.get(prevCur)
			if len(errs) > 0 {
				ret.Errors = append(ret.Errors, errs...)
				return
			}
			if loaded {
				if hold[c].query.saveName == "" {
					hold[c].sentCurrentToArray = false
					hold[c].currentIDObject = byt
				} else {
					hold[c].sentCurrentToArray = false
					hold[c].currentObject = obj
				}

				if !hold[a].sentCurrentToArray {
					if hold[a].query.skip > hold[a].skiped {
						hold[a].skiped++
					} else {
						hold[a].sentCurrentToArray = true
						hold[a].count++
						if hold[a].query.saveName != "" {
							hold[a].dataObject[string(hold[a].currentObject[KeyValueKey{Main: "ID"}])] = hold[a].currentObject
						} else {
							hold[a].idsObject[string(hold[a].currentIDObject)] = struct{}{}
						}
					}
					if hold[a].count >= hold[a].query.limit {
						position = -1
					}
				}

				if !hold[b].sentCurrentToArray {
					if hold[b].query.skip > hold[b].skiped {
						hold[b].skiped++
					} else {
						hold[b].sentCurrentToArray = true
						hold[b].count++
						if hold[b].query.saveName != "" {
							hold[b].dataObject[string(hold[b].currentObject[KeyValueKey{Main: "FROM"}])+string(hold[b].currentObject[KeyValueKey{Main: "TO"}])] = hold[b].currentObject
						} else {
							hold[b].idsLink[string(hold[b].currentIDLink.FROM)+string(hold[b].currentIDLink.TO)] = hold[b].currentIDLink
						}
					}
					if hold[b].count >= hold[b].query.limit {
						position = -1
					}
				}

				if !hold[c].sentCurrentToArray {
					if hold[c].query.skip > hold[c].skiped {
						hold[c].skiped++
					} else {
						hold[c].sentCurrentToArray = true
						hold[c].count++
						if hold[c].query.saveName != "" {
							hold[c].dataObject[string(hold[c].currentObject[KeyValueKey{Main: "ID"}])] = hold[c].currentObject
						} else {
							hold[c].idsObject[string(hold[c].currentIDObject)] = struct{}{}
						}
					}
					if hold[c].count >= hold[c].query.limit {
						position = -1
					}
				}

				if position != -1 {
					position = 2
				}

				down = false

			} else {
				position = 2
				down = false
			}
		}

		if position == -1 {
			if hold[c].query.saveName == "" {
				for key := range hold[c].idsObject {
					hold[c].loadedKeys = append(hold[c].loadedKeys, key)
				}
			} else {
				for key := range hold[c].dataObject {
					hold[c].loadedKeys = append(hold[c].loadedKeys, key)
				}
			}
			a = a + 2
			b = b + 2
			c = c + 2
			position = 1
			if len(hold) < c {
				do = false
			}
		}

	}

	//Log.Print("Done with this one")

	for _, v := range hold {
		if v.query.saveName != "" {
			d := make([]map[KeyValueKey][]byte, 0)
			for _, vv := range v.dataObject {
				d = append(d, vv)
			}
			if v.query.Direction != "" { //if it is a link
				l := LinkList{}
				l.LinkName = v.query.TypeName
				l.isIds = false
				l.Links = d
				l.order.field = v.query.Sort
				l.order.typ = v.query.SortType
				l.fields = v.query.Fields
				(*data)[v.query.saveName] = &l
			} else {
				o := ObjectList{}
				o.ObjectName = v.query.TypeName
				o.isIds = false
				o.Objects = d
				o.order.field = v.query.Sort
				o.order.typ = v.query.SortType
				o.fields = v.query.Fields
				(*data)[v.query.saveName] = &o
			}
		}
	}

}

//GetterGraphStraight ..
//action: 'objects'
//params... NodeQuery (NodeQuery)
//placesses objectLists found in  node query [0] in variable [1]
func GetterGraphStraightObjectStart(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	hold := make([]holder, len(qData))
	defer func() {
		for i := range hold {
			if hold[i].link != nil {
				hold[i].link.close()
			}
		}
	}()

	for i, v := range qData {
		var n NodeQuery
		var ok bool
		var h holder
		if i != 0 {
			n, ok = v.(NodeQuery)
			if !ok {
				ret.Errors = append(ret.Errors, errors.New("invalid argument provided in graph.p expected Nodequery"))
			}
			h = holder{}
			h.query = &n
			if i == (len(qData) - 1) {
				if h.query.limit == 0 {
					h.query.limit = 100
				}
			}
		} else {
			h = holder{}
		}

		j := i + 1
		if j == 1 {
			ke, ok := v.(string)
			if !ok {
				ret.Errors = append(ret.Errors, errors.New("invalid first argument provided in graph.p.o expected string"))
				return
			}
			var objectList *ObjectList
			objectList, ok = (*data)[ke].(*ObjectList)
			if !ok {
				ret.Errors = append(ret.Errors, errors.New("invalid data for first argument provided in graph.p.o expecting *objectlist"))
				return
			}
			objs := make([]map[KeyValueKey][]byte, 0)
			objs = append(objs, objectList.Objects...)
			h.dataObjectFirst = objs
			n := NodeQuery{}
			n.TypeName = objectList.ObjectName
			n.saveName = ke + "new"
			n.limit = len(objectList.Objects) + 1
			n.Sort = objectList.order.field
			n.SortType = objectList.order.typ
			n.Fields = objectList.fields
			h.query = &n
		}
		if j%2 == 0 {
			s := iteratorLoaderGraphLink{node: h.query, g: g, txn: txn}
			h.link = &s
			if h.query.saveName != "" {
				m := make(map[string]map[KeyValueKey][]byte)
				h.dataObject = m
			} else {
				m := make(map[string]LinkListList)
				h.idsLink = m
			}
		}
		if j%2 != 0 && i != 1 {
			s := iteratorLoaderGraphObject{h.query, g, txn, false}
			h.object = &s
			if h.query.saveName != "" {
				m := make(map[string]map[KeyValueKey][]byte)
				h.dataObject = m
			} else {
				m := make(map[string]struct{})
				h.idsObject = m
			}
		}

		hold[i] = h
	}

	do := true
	position := 1
	down := true
	last := 0

	a := 0
	b := 1
	c := 2

	for do {
		// deal with the first node
		if position == 1 {

			if a == 0 {
				//execution came up so change current and if successfull go back down if not call it a day
				//Log.Print("---------------------------- " + strconv.Itoa(a+1))

				if len(hold[a].dataObjectFirst) == 0 {
					position = -1
				} else {
					object := hold[a].dataObjectFirst[0]
					hold[a].dataObjectFirst = hold[a].dataObjectFirst[1:]
					hold[a].currentObject = object
					hold[a].sentCurrentToArray = false
					position = 2
					down = true
				}
			}

			if a != 0 { // if it an object query that is not the first and the last
				//Log.Print("----------------------------- " + strconv.Itoa(a+1))
				var key string
				if len(hold[a].loadedKeys) > 0 {
					key, hold[a].loadedKeys = hold[a].loadedKeys[len(hold[a].loadedKeys)-1], hold[a].loadedKeys[:len(hold[a].loadedKeys)-1]
				}

				if key == "" {
					position = -1
				} else {
					if hold[a].query.saveName == "" {
						hold[a].currentIDObject = []byte(key)
					} else {
						hold[a].currentObject = hold[a].dataObject[key]
					}
					hold[position].sentCurrentToArray = false
					position = 2
					down = true
				}
			}
		}

		if position == 2 { // if it is a link

			if down {
				//Log.Print("----------------------------- " + strconv.Itoa(b+1))
				var prevCur []byte

				if hold[b].query.Direction != "-" {
					g.DB.RLock()
					fromName := hold[a].query.TypeName
					lt := g.DB.LT[hold[b].query.TypeName]
					toName := hold[c].query.TypeName
					g.DB.RUnlock()
					if hold[b].query.Direction == "->" {
						if fromName != lt.From {
							ret.Errors = append(ret.Errors, errors.New("Link of type "+hold[b].query.TypeName+" with direction -> does not support this 'FROM' -'"+fromName+"'-"))
							return
						}
						if toName != lt.To {
							ret.Errors = append(ret.Errors, errors.New("Link of type "+hold[b].query.TypeName+" with direction -> does not support this 'TO' -'"+toName+"'-"))
							return
						}
					} else if hold[b].query.Direction == "<-" {
						if fromName != lt.To {
							ret.Errors = append(ret.Errors, errors.New("Link of type "+hold[b].query.TypeName+" with direction <- does not support this 'FROM' -'"+fromName+"'-"))
							return
						}
						if toName != lt.From {
							ret.Errors = append(ret.Errors, errors.New("Link of type "+hold[b].query.TypeName+" with direction <- does not support this 'TO' -'"+toName+"'-"))
							return
						}
					}
				}

				if hold[a].query.saveName == "" {
					prevCur = hold[a].currentIDObject
				} else {
					prevCur = hold[a].currentObject[KeyValueKey{Main: "ID"}]
				}
				if prevCur == nil {
					ret.Errors = append(ret.Errors, errors.New("Invalid previous id provided from nodequery "+fmt.Sprint(a)+", ID: "+string(prevCur)))
					return
				}
				obj, link, errs, loaded := hold[b].link.get2(prevCur)
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
					return
				}
				if loaded {
					if hold[b].query.saveName == "" {
						hold[b].currentIDLink = link
						//f, _ := binary.Uvarint(link.FROM)
						//t, _ := binary.Uvarint(link.TO)
						//Log.Print(strconv.Itoa(int(f)) + " ____ " + hold[b].link.currentDirection + " ____ " + strconv.Itoa(int(t)))
					} else {
						hold[b].currentObject = obj
						//f, _ := binary.Uvarint(obj[KeyValueKey{Main: "FROM"}])
						//t, _ := binary.Uvarint(obj[KeyValueKey{Main: "TO"}])
						//Log.Print(strconv.Itoa(int(f)) + " ____ " + hold[b].link.currentDirection + " ____ " + strconv.Itoa(int(t)))
					}
					hold[b].sentCurrentToArray = false
					position = 3
					down = true
				} else {
					// Unable to get even one object so go to the previous node to change prevCur
					position = 1
				}
			} else {
				//Log.Print("----------------------------- " + strconv.Itoa(b+1))
				obj, link, errs, loaded := hold[b].link.more2()
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
					return
				}
				if loaded {
					if hold[b].query.saveName == "" {
						hold[b].currentIDLink = link
					} else {
						hold[b].currentObject = obj
					}
					hold[b].sentCurrentToArray = false
					position = 3
					down = true
				} else {
					// Unable to get even one object so go to the previous node to change prevCur
					position = 1
				}
			}
		}

		if position == 3 { // if the last query, that is definitely an object query
			//Log.Print("----------------------------- " + strconv.Itoa(c+1))
			last++
			var prevCur []byte
			if hold[b].query.saveName == "" {
				prevCur = hold[b].currentIDLink.TO
			} else {
				prevCur = hold[b].currentObject[KeyValueKey{Main: "TO"}]
			}
			if prevCur == nil {
				ret.Errors = append(ret.Errors, errors.New("Invalid previous id provided from nodequery "+fmt.Sprint(b)+", ID: "+string(prevCur)))
				return
			}
			obj, byt, errs, loaded := hold[c].object.get(prevCur)
			if len(errs) > 0 {
				ret.Errors = append(ret.Errors, errs...)
				return
			}
			if loaded {
				if hold[c].query.saveName == "" {
					hold[c].sentCurrentToArray = false
					hold[c].currentIDObject = byt
				} else {
					hold[c].sentCurrentToArray = false
					hold[c].currentObject = obj
				}

				if !hold[a].sentCurrentToArray {
					if hold[a].query.skip > hold[a].skiped {
						hold[a].skiped++
					} else {
						hold[a].sentCurrentToArray = true
						hold[a].count++
						if hold[a].query.saveName != "" {
							hold[a].dataObject[string(hold[a].currentObject[KeyValueKey{Main: "ID"}])] = hold[a].currentObject
						} else {
							hold[a].idsObject[string(hold[a].currentIDObject)] = struct{}{}
						}
					}
					if hold[a].count >= hold[a].query.limit {
						position = -1
					}
				}

				if !hold[b].sentCurrentToArray {
					if hold[b].query.skip > hold[b].skiped {
						hold[b].skiped++
					} else {
						hold[b].sentCurrentToArray = true
						hold[b].count++
						if hold[b].query.saveName != "" {
							hold[b].dataObject[string(hold[b].currentObject[KeyValueKey{Main: "FROM"}])+string(hold[b].currentObject[KeyValueKey{Main: "TO"}])] = hold[b].currentObject
						} else {
							hold[b].idsLink[string(hold[b].currentIDLink.FROM)+string(hold[b].currentIDLink.TO)] = hold[b].currentIDLink
						}
					}
					if hold[b].count >= hold[b].query.limit {
						position = -1
					}
				}

				if !hold[c].sentCurrentToArray {
					if hold[c].query.skip > hold[c].skiped {
						hold[c].skiped++
					} else {
						hold[c].sentCurrentToArray = true
						hold[c].count++
						if hold[c].query.saveName != "" {
							hold[c].dataObject[string(hold[c].currentObject[KeyValueKey{Main: "ID"}])] = hold[c].currentObject
						} else {
							hold[c].idsObject[string(hold[c].currentIDObject)] = struct{}{}
						}
					}
					if hold[c].count >= hold[c].query.limit {
						position = -1
					}
				}

				if position != -1 {
					position = 2
				}
				down = false

			} else {
				position = 2
				down = false
			}
		}

		if position == -1 {
			if hold[c].query.saveName == "" {
				for key := range hold[c].idsObject {
					hold[c].loadedKeys = append(hold[c].loadedKeys, key)
				}
			} else {
				for key := range hold[c].dataObject {
					hold[c].loadedKeys = append(hold[c].loadedKeys, key)
				}
			}
			a = a + 2
			b = b + 2
			c = c + 2
			position = 1
			if len(hold) < c {
				do = false
			}
		}

	}

	//Log.Print("Done with this one")

	for _, v := range hold {
		if v.query.saveName != "" {
			d := make([]map[KeyValueKey][]byte, 0)
			for _, vv := range v.dataObject {
				d = append(d, vv)
			}
			if v.query.Direction != "" { //if it is a link
				l := LinkList{}
				l.LinkName = v.query.TypeName
				l.isIds = false
				l.Links = d
				l.order.field = v.query.Sort
				l.order.typ = v.query.SortType
				l.fields = v.query.Fields
				(*data)[v.query.saveName] = &l
			} else {
				o := ObjectList{}
				o.ObjectName = v.query.TypeName
				o.isIds = false
				o.Objects = d
				o.order.field = v.query.Sort
				o.order.typ = v.query.SortType
				o.fields = v.query.Fields
				(*data)[v.query.saveName] = &o
			}
		}
	}

}

func getObjectsForSingleObject(g *GetterFactory, txn *badger.Txn, fromName string, ID []byte, linkq, objectq *NodeQuery) (data []map[string]interface{}, errs []error) {

	linkh := holder{}
	linkh.query = linkq
	linkh.query.saveName = ""
	linkh.link = &iteratorLoaderGraphLink{node: linkq, g: g, txn: txn}
	m := make(map[string]LinkListList)
	linkh.idsLink = m

	objecth := holder{}
	objecth.query = objectq
	objecth.object = &iteratorLoaderGraphObject{objectq, g, txn, false}
	m2 := make(map[string]map[KeyValueKey][]byte)
	objecth.dataObject = m2
	defer func() {
		if linkh.link != nil && linkh.link.iterator != nil {
			linkh.link.iterator.Close()
		}
	}()
	do := true
	position := 1
	down := true

	for do {
		// deal with the first node

		if position == 1 { // if it is a link

			if down {

				if linkh.query.Direction != "-" {
					g.DB.RLock()
					lt := g.DB.LT[linkh.query.TypeName]
					toName := objecth.query.TypeName
					g.DB.RUnlock()
					if linkh.query.Direction == "->" {
						if fromName != lt.From {
							errs = append(errs, errors.New("Link of type "+linkh.query.TypeName+" with direction -> does not support this 'FROM' -'"+fromName+"'-"))
							return
						}
						if toName != lt.To {
							errs = append(errs, errors.New("Link of type "+linkh.query.TypeName+" with direction -> does not support this 'TO' -'"+toName+"'-"))
							return
						}
					} else if linkh.query.Direction == "<-" {
						if fromName != lt.To {
							errs = append(errs, errors.New("Link of type "+linkh.query.TypeName+" with direction <- does not support this 'FROM' -'"+fromName+"'-"))
							return
						}
						if toName != lt.From {
							errs = append(errs, errors.New("Link of type "+linkh.query.TypeName+" with direction <- does not support this 'TO' -'"+toName+"'-"))
							return
						}
					}
				}

				if ID == nil {
					errs = append(errs, errors.New("Invalid previous id provided from nodequery ID: "+string(ID)))
					return
				}
				_, link, ers, loaded := linkh.link.get2(ID)
				if len(ers) > 0 {
					errs = append(errs, ers...)
					return
				}
				if loaded {
					linkh.currentIDLink = link
					linkh.sentCurrentToArray = false
					position = 2
					down = true
				} else {
					// Unable to get even one object so return empty map
					data = make([]map[string]interface{}, 0)
					return
				}
			} else {
				_, link, ers, loaded := linkh.link.more2()
				if len(ers) > 0 {
					errs = append(errs, ers...)
					return
				}
				if loaded {
					linkh.currentIDLink = link
					linkh.sentCurrentToArray = false
					position = 2
					down = true
				} else {
					// Unable to get even one object so go to the previous node to change prevCur
					do = false
				}
			}
		}

		if position == 2 { // if the last query, that is definitely an object query
			var prevCur []byte
			prevCur = linkh.currentIDLink.TO
			if prevCur == nil {
				errs = append(errs, errors.New("Invalid previous id provided from nodequery , ID: "+string(prevCur)))
				return
			}
			obj, _, ers, loaded := objecth.object.get(prevCur)
			if len(ers) > 0 {
				errs = append(errs, ers...)
				return
			}
			if loaded {
				objecth.sentCurrentToArray = false
				objecth.currentObject = obj

				if !objecth.sentCurrentToArray {
					if objecth.query.skip > objecth.skiped {
						objecth.skiped++
					} else {
						objecth.sentCurrentToArray = true
						objecth.count++
						objecth.dataObject[string(objecth.currentObject[KeyValueKey{Main: "ID"}])] = objecth.currentObject
					}
					if objecth.count >= objecth.query.limit {
						do = false
						position = -1
					}
				}

			}
			position = 1
			down = false
		}

	}

	if len(objecth.dataObject) > 0 {
		d := make([]map[KeyValueKey][]byte, 0)
		for _, v := range objecth.dataObject {
			d = append(d, v)
		}
		o := ObjectList{}
		o.ObjectName = objecth.query.TypeName
		o.isIds = false
		o.Objects = d
		o.order.field = objecth.query.Sort
		o.order.typ = objecth.query.SortType
		o.fields = objecth.query.Fields
		m, ers := g.getObjectMapArray(&o)
		if len(ers) > 0 {
			errs = ers
			return
		}
		data = m
		return
	}
	data = make([]map[string]interface{}, 0)
	return
}

//GetterGraphStraight ..
//action: 'objects'
//params... NodeQuery (NodeQuery)
//placesses objectLists found in  node query [0] in variable [1]
func GetterGraphEmbeded(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {

	if len(qData) != 5 && len(qData) != 7 {
		ret.Errors = append(ret.Errors, errors.New("invalid number of arguments provided in embeded expected 5 or 7 "))
		return
	}
	saveName, ok := qData[0].(string)
	if !ok {
		ret.Errors = append(ret.Errors, errors.New("invalid first argument provided in embeded expected string"))
		return
	}
	objectName, ok := qData[1].(string)
	if !ok {
		ret.Errors = append(ret.Errors, errors.New("invalid second argument provided in embeded expected string"))
		return
	}

	fromName, ok := qData[2].(string)
	if !ok {
		ret.Errors = append(ret.Errors, errors.New("invalid third argument provided in embeded expected string"))
		return
	}
	var retData []map[string]interface{}

	objectList, ok := (*data)[objectName]

	if !ok {
		ret.Errors = append(ret.Errors, errors.New("provided object name is not pointing to any existing data"))
		return
	}

	switch x := objectList.(type) {
	case *ObjectList:
		var errs []error
		fromName = x.ObjectName
		retData, errs = g.getObjectMapArray(x)
		if len(errs) > 0 {
			ret.Errors = append(ret.Errors, errs...)
			return
		}
	case []map[string]interface{}:
		retData = x
	default:
		ret.Errors = append(ret.Errors, errors.New("provided object not of type []map[string]interface{} or *ObjectList"))
		return
	}

	if len(retData) == 0 {
		(*data)[saveName] = make([]map[string]interface{}, 0)
	}
	var nodes []NodeQuery
	for _, v := range qData[3:] {
		n, ok := v.(NodeQuery)
		if !ok {
			ret.Errors = append(ret.Errors, errors.New("provided parameter not type of NodeQuery "))
			return
		}
		nodes = append(nodes, n)
	}

	for i, v := range retData {
		idu, ok := v["ID"].(uint64)
		if !ok {
			ret.Errors = append(ret.Errors, errors.New("invalid ID gotten from data loaded in embeded "))
			return
		}
		g.DB.RLock()
		idb, _ := g.DB.FT["uint64"].Set(idu)
		g.DB.RUnlock()
		subdata, errs := getObjectsForSingleObject(g, txn, fromName, idb, &nodes[0], &nodes[1])
		if len(errs) > 0 {
			ret.Errors = append(ret.Errors, errs...)
			return
		}
		if len(nodes) == 4 {
			for i2, v2 := range subdata {
				idu2, ok2 := v2["ID"].(uint64)
				if !ok2 {
					ret.Errors = append(ret.Errors, errors.New("invalid ID gotten from data loaded in embeded "))
					return
				}
				g.DB.RLock()
				idb2, _ := g.DB.FT["uint64"].Set(idu2)
				g.DB.RUnlock()
				subsubdata, errs := getObjectsForSingleObject(g, txn, nodes[1].TypeName, idb2, &nodes[2], &nodes[3])
				if len(errs) > 0 {
					ret.Errors = append(ret.Errors, errs...)
					return
				}
				subdata[i2][nodes[2].TypeName] = subsubdata
			}
		}
		retData[i][nodes[0].TypeName] = subdata
	}

	//log.Print("ooooo", retData)

	(*data)[saveName] = retData

}

func InternalInstruction(g *GetterFactory, txn *badger.Txn, data *map[string]interface{}, q *Query, qData []interface{}, ret *GetterRet) {
	defer func() {
		q.Ret <- *ret
	}()
	if len(qData) < 1 {
		ret.Errors = append(ret.Errors, errors.New("less than 1 arguments given in Internal instruction"))
		return
	}
	one, ok := qData[0].(string)
	if !ok {
		ret.Errors = append(ret.Errors, errors.New("invalid first argument provided in mergeobjectlist"))
		return
	}

	switch one {
	case "update-index":
		if len(qData) < 3 {
			ret.Errors = append(ret.Errors, errors.New("less than 3 arguments given in update-index  Internal instruction"))
			return
		}
		object, ok := qData[1].(string)
		if !ok {
			ret.Errors = append(ret.Errors, errors.New("invalid 2 argument provided in internal instruction"))
			return
		}
		field, ok := qData[2].(string)
		if !ok {
			ret.Errors = append(ret.Errors, errors.New("invalid 3 argument provided in internal instruction"))
			return
		}
		last, err := g.DB.KV.GetNextID(object, 10)
		g.DB.RLock()
		defer g.DB.RUnlock()
		if err != nil {
			ret.Errors = append(ret.Errors, errors.New("unable to get next sequence for object"))
			return
		}
		for i := uint64(1); i < last; i++ {
			inRaw, _ := g.DB.FT["uint64"].Set(i)
			key := g.DB.Options.DBName + g.DB.Options.DBDelimiter + object + g.DB.Options.DBDelimiter + string(inRaw) + g.DB.Options.DBDelimiter + field
			item, err := txn.Get([]byte(key))
			if err == nil {
				value, err := item.ValueCopy(nil)
				if err == nil {
					g.DB.KV.Writer2.Write(value, g.DB.Options.DBName, object, field, string(value), string(inRaw))
				} else {
					ret.Errors = append(ret.Errors, errors.New("error getting value for key: "+key), err)
				}
			} else if err != badger.ErrKeyNotFound {
				ret.Errors = append(ret.Errors, err)
			}
		}

	case "delete-index":
		if len(qData) < 3 {
			ret.Errors = append(ret.Errors, errors.New("less than 3 arguments given in delete-index  Internal instruction"))
			return
		}
		object, ok := qData[1].(string)
		if !ok {
			ret.Errors = append(ret.Errors, errors.New("invalid 2 argument provided in delete-index internal instruction"))
			return
		}
		field, ok := qData[2].(string)
		if !ok {
			ret.Errors = append(ret.Errors, errors.New("invalid 3 argument provided in delete-index internal instruction"))
			return
		}
		last, err := g.DB.KV.GetNextID(object, 10)
		g.DB.RLock()
		defer g.DB.RUnlock()
		if err != nil {
			ret.Errors = append(ret.Errors, errors.New("unable to get next sequence for object"))
			return
		}
		for i := uint64(1); i < last; i++ {
			inRaw, _ := g.DB.FT["uint64"].Set(i)
			key := g.DB.Options.DBName + g.DB.Options.DBDelimiter + object + g.DB.Options.DBDelimiter + string(inRaw) + g.DB.Options.DBDelimiter + field
			item, err := txn.Get([]byte(key))
			if err == nil {
				value, err := item.ValueCopy(nil)
				if err == nil {
					g.DB.KV.Writer2.Delete(g.DB.Options.DBName, object, field, string(value), string(inRaw))
				} else {
					ret.Errors = append(ret.Errors, errors.New("error getting value for key: "+key), err)
				}
			} else if err != badger.ErrKeyNotFound {
				ret.Errors = append(ret.Errors, err)
			}
		}

	default:
		ret.Errors = append(ret.Errors, errors.New("Invalid Instruction provided for internal instructioin: "+one))
		return
	}

}
