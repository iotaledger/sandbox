package main

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/iotaledger/giota"
)

type IRIBalancer struct {
	mu      sync.Mutex // Protect the healthy nodes slice.
	nodes   []*giota.API
	healthy []int // Indices of healthy nodes
	next    int   // Index for the next healthy node index to return, i.e.
	// nodes[healthy[next]]

	done   chan struct{}
	ticker *time.Ticker
}

func NewIRIBalancer(urls []string) *IRIBalancer {
	ib := &IRIBalancer{}
	nodes := []*giota.API{}
	healthy := []int{}
	for i, u := range urls {
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
		healthy = append(healthy, i)
	}

	return ib
}

// Start checking the health of the given node every interval.
func (ib *IRIBalancer) Start(interval time.Duration) error {
	if interval < 0 {
		return fmt.Errorf("invalid interval duration")
	}

	ib.done = make(chan struct{})
	ib.ticker = time.NewTicker(interval)

	go func() {
		for {
			select {
			case _ = <-ib.ticker.C:
				if err := ib.checkNodes(); err != nil {
					return
				}
			case <-ib.done:
				return
			}
		}
	}()

	return nil
}

func (ib *IRIBalancer) checkNodes() error {
	ib.mu.Lock()
	defer ib.mu.Unlock()

	healthy := []int{}
	for i, n := range ib.nodes {
		if err := ib.isHealthyNode(n); err == nil {
			healthy = append(healthy, i)
		}
	}

	if ib.next > len(healthy) {
		ib.next = 0
	}

	ib.healthy = healthy

	return nil
}

func (ib *IRIBalancer) isHealthyNode(n *giota.API) error {
	if n == nil {
		return fmt.Errorf("received nil node")
	}

	// XXX: Consider coming up with better metrics for node health.
	if _, err := n.GetNodeInfo(); err != nil {
		return err
	}

	return nil
}

// Get the next available node or return an error if none are available.
func (ib *IRIBalancer) Get() (*giota.API, error) {
	// XXX: Maybe this methdd should block.
	// 			If it were to block then it should take a context to let the
	// 			caller specify a timeout/deadline.
	ib.mu.Lock()
	defer ib.mu.Unlock()

	if len(ib.healthy) == 0 {
		return nil, fmt.Errorf("no available nodes")
	}

	ib.next += 1
	if ib.next >= len(ib.healthy) {
		ib.next = 0
	}

	return ib.nodes[ib.healthy[ib.next]], nil
}

// Close shuts down the balancer.
func (ib *IRIBalancer) Close() error {
	ib.mu.Lock()
	defer ib.mu.Unlock()
	close(ib.done)
	ib.ticker.Stop()

	return nil
}
