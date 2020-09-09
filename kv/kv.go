package kv

type KV struct {
	ID     int
	Values map[int]struct{}
	Latest int
}

func NewKV(id int) KV {
	return KV{
		ID:     id,
		Values: make(map[int]struct{}),
		Latest: -1,
	}
}

func (k *KV) NewValue(s *Schema) {
	id := s.NewValue(k.ID)
	k.Latest = id
	k.Values[id] = struct{}{}
}

func (k *KV) PutValue(s *Schema) {
	// complete me
}

func (k *KV) DelValue(s *Schema) {
	// complete me
	k.Latest = -1
}
