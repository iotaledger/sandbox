package job

import (
	"errors"
	"sync"
	"time"

	//uuid "github.com/satori/go.uuid"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type JobStore interface {
	InsertJob(*IRIJob) (string, error)
	SelectJob(string) (*IRIJob, error)
	UpdateJob(string, *IRIJob) (*IRIJob, error)
	TimeoutJobs(time.Duration) error
}

var (
	ErrJobNotFound = errors.New("could not find job")
)

type MemoryStore struct {
	mut *sync.Mutex
	db  map[string]*IRIJob
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		mut: &sync.Mutex{},
		db:  make(map[string]*IRIJob),
	}
}

func (ms *MemoryStore) InsertJob(it *IRIJob) (string, error) {
	ms.mut.Lock()
	ms.db[it.ID] = it
	ms.mut.Unlock()

	return it.ID, nil
}

func (ms *MemoryStore) SelectJob(id string) (*IRIJob, error) {
	ms.mut.Lock()
	defer ms.mut.Unlock()

	if ms.db[id] == nil {
		return nil, ErrJobNotFound
	}

	return ms.db[id], nil
}

func (ms *MemoryStore) UpdateJob(id string, it *IRIJob) (*IRIJob, error) {
	ms.mut.Lock()
	defer ms.mut.Unlock()

	ms.db[id] = it

	return ms.db[id], nil
}

func (ms *MemoryStore) TimeoutJobs(d time.Duration) error {
	ms.mut.Lock()
	defer ms.mut.Unlock()
	t := time.Now().Add(-d).Unix()

	for k, _ := range ms.db {
		if ms.db[k].CreatedAt <= t && ms.db[k].Status == JobStatusQueued {
			ms.db[k].Status = JobStatusFailed
			ms.db[k].Error = &JobError{Message: "timed out"}
		}
	}

	return nil
}

type MongoStore struct {
	session    *mgo.Session
	collection *mgo.Collection
}

func NewMongoStore(uri string, db string) (*MongoStore, error) {
	session, err := mgo.Dial(uri)
	if err != nil {
		return nil, err
	}
	session.SetSafe(&mgo.Safe{})
	c := session.DB(db).C("jobs")

	c.EnsureIndex(mgo.Index{Key: []string{"id"}, Unique: true, DropDups: false})

	m := &MongoStore{session: session, collection: c}

	return m, nil
}

func (ms *MongoStore) InsertJob(it *IRIJob) (string, error) {
	err := ms.collection.Insert(it)
	if err != nil {
		return "", err
	}
	return it.ID, nil
}

func (ms *MongoStore) SelectJob(id string) (*IRIJob, error) {
	var r IRIJob
	err := ms.collection.Find(bson.M{"id": id}).One(&r)
	switch {
	case err == mgo.ErrNotFound:
		return nil, ErrJobNotFound
	case err != nil:
		return nil, err
	default:
		return &r, nil
	}
}

func (ms *MongoStore) UpdateJob(id string, it *IRIJob) (*IRIJob, error) {
	err := ms.collection.Update(bson.M{"id": id}, it)
	if err != nil {
		return nil, err
	}
	return it, nil
}

func (ms *MongoStore) TimeoutJobs(d time.Duration) error {
	t := time.Now().Add(-d).Unix()
	_, err := ms.collection.UpdateAll(
		bson.M{
			"createdAt": bson.M{"$lte": t},
			"status":    JobStatusQueued,
		},
		bson.M{
			"$set": bson.M{
				"status": JobStatusFailed,
				"error":  bson.M{"message": "timed out"},
			},
		},
	)

	return err
}
