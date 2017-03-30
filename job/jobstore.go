package job

import (
	"errors"
	"fmt"
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
	JobFailureRate(time.Duration) (int64, float64)
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

func (ms *MemoryStore) JobFailureRate(d time.Duration) (int64, float64) {
	ms.mut.Lock()
	defer ms.mut.Unlock()
	t := time.Now().Add(-d).Unix()
	failed := 0.0
	finished := 0.0
	count := int64(0)

	for k, _ := range ms.db {
		if ms.db[k].CreatedAt >= t {
			switch ms.db[k].Status {
			case JobStatusFinished:
				finished += 1.0
			case JobStatusFailed:
				failed += 1.0
			}
			count += 1
		}
	}

	if failed+finished == 0.0 {
		return count, 0.0
	}

	return count, failed / (failed + finished)
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

func (ms *MongoStore) JobFailureRate(d time.Duration) (int64, float64) {
	t := time.Now().Add(d).Unix()
	failed := 0.0
	finished := 0.0
	count := int64(0)

	p := ms.collection.Pipe(
		[]bson.M{
			{"$match": bson.M{"createdAt": bson.M{"$gte": t}}},
			{"$group": bson.M{"_id": "$status", "counts": bson.M{"$sum": 1}}},
		},
	)
	if p == nil {
		return 0, 0.0 // ?
	}

	var s struct {
		Status string `bson:"_id"`
		Counts int64  `bson:"counts"`
	}

	it := p.Iter()

	for it.Next(&s) {
		fmt.Printf("s: %#v\n", s)
		switch s.Status {
		case JobStatusFailed:
			failed = float64(s.Counts)
		case JobStatusFinished:
			finished = float64(s.Counts)
		}
		count += s.Counts
	}

	if failed+finished == 0.0 {
		return count, 0.0
	}

	return count, failed / (failed + finished)
}
