package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	giota "github.com/iotaledger/iota.lib.go"
)

type IRIBalancer struct {
	mu        sync.Mutex
	healthy   map[string]*giota.API
	unhealthy map[string]*giota.API
}

func NewIRIBalancer(urls []string) *IRIBalancer {
	ib := &IRIBalancer{
		healthy:   map[string]*giota.API{},
		unhealthy: map[string]*giota.API{},
	}
	nodes := []*giota.API{}
	for _, u := range urls {
		tr := &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
		client := &http.Client{
			Transport: tr,
		}
		nodes = append(nodes, giota.NewAPI(u, client))
	}

	return ib
}

func (ib *IRIBalancer) Start() error {
	t := time.NewTicker(time.Second)
	for _ = range t.C {
		fmt.Printf("tick\n")
	}
}

func (ib *IRIBalancer) isHealthy()

func (ib *IRIBalancer) Get(ctx context.Context)

func (ib *IRIBalancer) Close() error {}
