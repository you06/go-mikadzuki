package kv

type KV struct {
	ID     int
	Values map[int]struct{}
}

func NewKV(id int) KV {
	return KV{
		ID:     id,
		Values: make(map[int]struct{}),
	}
}

func (k *KV) NewValue(s *Schema) {
	id := s.NewValue(k.ID)
	k.Values[id] = struct{}{}
}
