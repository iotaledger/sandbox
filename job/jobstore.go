package job

import (
	"sync"

	uuid "github.com/satori/go.uuid"
)

type JobStore interface {
	InsertJob(*IRIJob) (uuid.UUID, error)
	SelectJob(uuid.UUID) (*IRIJob, error)
	UpdateJob(uuid.UUID, *IRIJob) (*IRIJob, error)
}

type MemoryStore struct {
	mut *sync.Mutex
	db  map[uuid.UUID]*IRIJob
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		mut: &sync.Mutex{},
		db:  make(map[uuid.UUID]*IRIJob),
	}
}

func (ms *MemoryStore) InsertJob(it *IRIJob) (uuid.UUID, error) {
	ms.mut.Lock()
	ms.db[it.ID] = it
	ms.mut.Unlock()

	return it.ID, nil
}

func (ms *MemoryStore) SelectJob(id uuid.UUID) (*IRIJob, error) {
	ms.mut.Lock()
	defer ms.mut.Unlock()

	return ms.db[id], nil
}

func (ms *MemoryStore) UpdateJob(id uuid.UUID, it *IRIJob) (*IRIJob, error) {
	ms.mut.Lock()
	defer ms.mut.Unlock()

	ms.db[id] = it

	return ms.db[id], nil
}
