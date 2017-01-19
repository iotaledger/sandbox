package main

import (
	//uuid "github.com/satori/go.uuid"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Auth struct {
	Token      string `json:"token"`
	Active     bool   `json:"active" bson:"active"`
	EMail      string `json:"email" bson:"email"`
	CreatedAt  int64  `json:"createdAt"`
	ModifiedAt int64  `json:"modifiedAt"`
}

// AuthStore describes backend
type AuthStore interface {
	ValidToken(tok string) (bool, error)
}

// DummyStore just accepts any non empty token as valid.
type DummyStore struct{}

func NewDummyStore() (*DummyStore, error) {
	return &DummyStore{}, nil
}

func (d *DummyStore) ValidToken(tok string) (bool, error) {
	return tok != "", nil
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
	c := session.DB(db).C("credentials")

	c.EnsureIndex(mgo.Index{Key: []string{"token"}, Unique: true, DropDups: false})

	m := &MongoStore{session: session, collection: c}

	return m, nil
}

func (m *MongoStore) ValidToken(tok string) (bool, error) {
	var r Auth
	err := m.collection.Find(bson.M{"active": true, "token": tok}).One(&r)
	switch {
	case err == mgo.ErrNotFound:
		return false, nil
	case err != nil:
		return false, err
	default:
		return r.Active, nil
	}
}
