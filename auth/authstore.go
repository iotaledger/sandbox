package auth

import (
	"context"
	//uuid "github.com/satori/go.uuid"
	"cloud.google.com/go/datastore"
	"google.golang.org/api/option"
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
	ValidToken(ctx context.Context, tok string) (bool, error)
	Add(ctx context.Context, a *Auth) error
	Get(ctx context.Context, tok string) (*Auth, error)
}

// DummyStore just accepts any non empty token as valid.
type DummyStore struct{}

func NewDummyStore() (*DummyStore, error) {
	return &DummyStore{}, nil
}

func (d *DummyStore) ValidToken(_ context.Context, tok string) (bool, error) {
	return tok != "", nil
}

func (d *DummyStore) Add(_ context.Context, a *Auth) error {
	return nil
}

func (d *DummyStore) Get(_ context.Context, tok string) (*Auth, error) {
	return &Auth{Token: tok}, nil
}

type GCloudDataStore struct {
	client *datastore.Client
}

func NewGCloudDataStore(projectID, credPath string) (*GCloudDataStore, error) {
	ctx := context.Background()
	var dsClient *datastore.Client
	if credPath != "" {
		c, err := datastore.NewClient(ctx, projectID, option.WithServiceAccountFile(credPath))
		if err != nil {
			return nil, err
		}
		dsClient = c
	} else {
		c, err := datastore.NewClient(ctx, projectID)
		if err != nil {
			return nil, err
		}
		dsClient = c
	}

	g := &GCloudDataStore{client: dsClient}
	return g, nil
}

func (g *GCloudDataStore) ValidToken(ctx context.Context, tok string) (bool, error) {
	a, err := g.Get(ctx, tok)
	if err != nil {
		return false, err
	}

	return a.Active, nil
}

func (g *GCloudDataStore) Add(ctx context.Context, a *Auth) error {
	k := datastore.NameKey("Auth", a.Token, nil)
	if _, err := g.client.Put(ctx, k, a); err != nil {
		return err
	}

	return nil
}

func (g *GCloudDataStore) Get(ctx context.Context, tok string) (*Auth, error) {
	a := &Auth{}
	k := datastore.NameKey("Auth", tok, nil)
	if err := g.client.Get(ctx, k, a); err != nil {
		return nil, err
	}

	return a, nil
}
