package raft

type StringSet struct {
	Items map[string]bool
}

func InitStringSet() StringSet {
	return StringSet{
		Items: map[string]bool{},
	}
}

func (s *StringSet) Add(item string) {
	if !s.Contains(item) {
		s.Items[item] = true
	}
}
func (s *StringSet) Contains(item string) bool {
	return s.Items[item]
}
func (s *StringSet) Remove(item string) {
	delete(s.Items, item)
}

func (s *StringSet) Length() int {
	return len(s.Items)
}

func (s *StringSet) Clear() {
	s.Items = map[string]bool{}
}
