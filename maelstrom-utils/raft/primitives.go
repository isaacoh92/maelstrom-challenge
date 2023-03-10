package raft

type StringSet struct {
	// Items []string
	// Keys  map[string]bool
	Items map[string]bool
}

func InitStringSet() StringSet {
	return StringSet{
		// Items: []string{},
		// Keys:  map[string]bool{},
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
	// if _, ok := s.Items[item]; ok {
	// 	s.Items[item] = false
	// }
	delete(s.Items, item)
}

func (s *StringSet) Length() int {
	return len(s.Items)
}

func (s *StringSet) Clear() {
	s.Items = map[string]bool{}
}

// func (s *StringSet) Get(item string) {

// }

/*
operation looks like

	{
		"type"       "read"
	 	"msg_id"     An integer
	 	"key"        A string: the key the client would like to read
	}

	{
		"type"     "write"
		"msg_id"   An integer
		"key"      A string: the key the client would like to write
		"value"    A string: the value the client would like to write
	}

	{
		"type"     "cas"
		"msg_id"   An integer
		"key"      A string: the key the client would like to write
		"from"     A string: the value that the client expects to be present
		"to"       A string: the value to write if and only if the value is `from`
		"create_if_not_exists" A bool: create the entry with `to` if it does not exist
	}
*/
// func (m *Map) Apply(operation map[string]any) (int, string, error) {
// 	key := operation["key"].(string)
// 	switch operation["type"].(string) {
// 	case "read":
// 		if val, ok := m.Data[key]; ok {
// 			return val, "read_ok", nil
// 		} else {
// 			return 0, "error", maelstrom.NewRPCError(ERR_KEY_DOES_NOT_EXIST, "key not found")
// 		}
// 	case "write":
// 		val, err := getInt(operation["value"])
// 		if err != nil {
// 			return 0, "error", err
// 		}
// 		m.Data[key] = val
// 		return val, "write_ok", nil
// 	case "cas":
// 		existingVal, ok := m.Data[key]
// 		if !ok {
// 			if operation["create_if_not_exists"].(bool) {
// 				m.Data[key] = operation["to"].(int)
// 				return m.Data[key], "cas_ok", nil
// 			}
// 			return 0, "error", maelstrom.NewRPCError(ERR_KEY_DOES_NOT_EXIST, "key not found")
// 		}

// 		if newVal := operation["from"].(int); existingVal != newVal {
// 			return 0, "error", maelstrom.NewRPCError(ERR_PRECONDITION_FAILED, fmt.Sprintf("cas expected %d; got %d", existingVal, newVal))
// 		}

// 		m.Data[key] = operation["to"].(int)
// 		return m.Data[key], "cas_ok", nil
// 	default:
// 		return 0, "error", maelstrom.NewRPCError(ERR_NOT_SUPPORTED, fmt.Sprintf("%s not supported", operation["type"].(string)))
// 	}
// }
