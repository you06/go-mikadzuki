package kv

import "math/rand"

const (
	NULL_VALUE_ID    = -1
	INVALID_VALUE_ID = -2
)

type KV struct {
	ID        int
	Values    map[int]struct{}
	Latest    int
	DeleteVal int
}

// Txn only record uncommitted values
// there are not conflict in Txn
type Txn struct {
	KID     int
	Latest  int
	kv      *KV
	History []KVAction
}

type Txns []*Txn

type KVActionTp int

const (
	KVActionNew KVActionTp = iota
	KVActionPut
	KVActionDel
)

type KVAction struct {
	Tp      KVActionTp
	ValueID int
}

func NewKV(id int) KV {
	return KV{
		ID:        id,
		Values:    make(map[int]struct{}),
		Latest:    NULL_VALUE_ID,
		DeleteVal: INVALID_VALUE_ID,
	}
}

func (k *KV) Begin() *Txn {
	return &Txn{
		KID:     k.ID,
		Latest:  k.Latest,
		kv:      k,
		History: []KVAction{},
	}
}

func (k *KV) GetValueNoTxn(s *Schema) string {
	var id int
	if k.Latest == NULL_VALUE_ID && k.DeleteVal != INVALID_VALUE_ID {
		id = k.DeleteVal
	} else {
		id = k.Latest
	}
	return s.SelectSQL(id)
}

func (k *KV) GetValueNoTxnWithID(s *Schema, vID int) string {
	return s.SelectSQL(vID)
}

func (k *KV) GetValueNoTxnForUpdateWithID(s *Schema, vID int) string {
	return s.SelectForUpdateSQL(vID)
}

func (k *KV) NewValueNoTxn(s *Schema) string {
	var v int
	if k.Latest == NULL_VALUE_ID && k.DeleteVal != INVALID_VALUE_ID {
		v = s.PutValue(k.ID, k.DeleteVal)
	} else {
		v = s.NewValue(k.ID)
	}
	k.Latest = v
	k.DeleteVal = INVALID_VALUE_ID
	return s.InsertSQL(v)
}

func (k *KV) PutValueNoTxn(s *Schema) string {
	oldID := k.Latest
	var newID int
	if k.Latest == NULL_VALUE_ID && k.DeleteVal != INVALID_VALUE_ID {
		newID = s.PutValue(k.ID, k.DeleteVal)
	} else {
		newID = s.PutValue(k.ID, k.Latest)
	}
	k.Latest = newID
	k.DeleteVal = INVALID_VALUE_ID
	return s.UpdateSQL(oldID, newID)
}

func (k *KV) DelValueNoTxn(s *Schema) string {
	id := k.Latest
	if id == NULL_VALUE_ID && k.DeleteVal != INVALID_VALUE_ID {
		id = k.DeleteVal
	} else {
		k.DeleteVal = id
	}
	k.Latest = NULL_VALUE_ID
	return s.DeleteSQL(id)
}

func (k *KV) ReplaceNoTxn(s *Schema, oldID int) string {
	newID := s.RepValue(k.ID, oldID)
	k.Latest = newID
	k.DeleteVal = INVALID_VALUE_ID
	return s.ReplaceSQL(newID)
}

func (k *KV) NewValue(v int) {
	k.Latest = v
	k.Values[v] = struct{}{}
}

func (k *KV) PutValue(v int) {
	k.Latest = v
	k.Values[v] = struct{}{}
}

func (k *KV) DelValue(v int) {
	k.Latest = NULL_VALUE_ID
	delete(k.Values, v)
}

func (t *Txn) NewValue(s *Schema) string {
	id := s.NewValue(t.kv.ID)
	t.Latest = id
	t.History = append(t.History, KVAction{
		Tp:      KVActionNew,
		ValueID: id,
	})
	return s.InsertSQL(id)
}

func (t *Txn) PutValue(s *Schema) string {
	oldID := t.Latest
	newID := s.PutValue(t.kv.ID, oldID)
	t.Latest = newID
	t.History = append(t.History, KVAction{
		Tp:      KVActionPut,
		ValueID: newID,
	})
	return s.UpdateSQL(oldID, newID)
}

func (t *Txn) DelValue(s *Schema) string {
	id := t.Latest
	t.Latest = NULL_VALUE_ID
	t.History = append(t.History, KVAction{
		Tp:      KVActionDel,
		ValueID: NULL_VALUE_ID,
	})
	return s.DeleteSQL(id)
}

// Commit apply mutations to KV
func (t *Txn) Commit() {
	for _, action := range t.History {
		switch action.Tp {
		case KVActionNew:
			t.kv.NewValue(action.ValueID)
		case KVActionPut:
			t.kv.PutValue(action.ValueID)
		case KVActionDel:
			t.kv.DelValue(action.ValueID)
		}
	}
}

// Rollback drops the actions in this txn
func (t *Txn) Rollback() {

}

func (ts *Txns) Push(t *Txn) {
	*ts = append(*ts, t)
}

func (ts *Txns) Last() *Txn {
	return (*ts)[len(*ts)-1]
}

func (ts *Txns) Commit() {
	for _, t := range *ts {
		t.Commit()
	}
}

func (ts *Txns) Rollback() {
	for _, t := range *ts {
		t.Rollback()
	}
}

func (ts *Txns) Rand() *Txn {
	l := len(*ts)
	if l == 0 {
		panic("get rand txn from empty txns")
	}
	return (*ts)[rand.Intn(l)]
}
