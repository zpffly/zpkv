package persister

import "github.com/syndtr/goleveldb/leveldb"

type Persister struct {
	db *leveldb.DB
}

func InitPersister(path string) *Persister {

	db, err := leveldb.OpenFile(path, nil)

	if err != nil {
		panic(err)
	}

	return &Persister{db: db}
}

func (p *Persister) SaveRaftState(key []byte, value []byte) error {
	return p.db.Put(key, value, nil)
}

func (p *Persister) ReadRaftState(key []byte) ([]byte, error) {
	return p.db.Get(key, nil)
}

func (p *Persister) Close() {
	p.db.Close()
}

func (p *Persister) Get(key []byte) ([]byte, error) {
	return p.db.Get(key, nil)
}

func (p *Persister) Put(key, val []byte) error {
	return p.db.Put(key, val, nil)
}

func (p *Persister) Del(key []byte) error {
	return p.db.Delete(key, nil)
}
