package persister

import "testing"

func TestPersister(t *testing.T) {
	persister := InitPersister("db")
	err := persister.Put([]byte("a"), []byte("1"))
	if err != nil {
		panic(err)
	}
	data, err := persister.Get([]byte("a"))
	if err != nil {
		panic(err)
	}
	t.Log(string(data))
}
