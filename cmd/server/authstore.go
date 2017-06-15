package main

import (
	"context"
	//uuid "github.com/satori/go.uuid"
	"cloud.google.com/go/datastore"
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

type GCloudDataStore struct {
	client *datastore.Client
}

func NewGCloudDataStore() (*GCloudDataStore, error) {
	ctx := context.Background()
	c, err := datastore.NewClient(ctx, "")
	if err != nil {
		return nil, err
	}

	g := &GCloudDataStore{client: c}
	return g, nil
}

func (g *GCloudDataStore) ValidToken(tok string) (bool, error) {
	ctx := context.Background()

	a := &Auth{}
	k := datastore.NameKey("Auth", tok, nil)
	if err := g.client.Get(ctx, k, a); err != nil {
		if err == datastore.ErrNoSuchEntity {
			return false, nil
		}

		return false, err
	}

	return a.Active, nil
}
