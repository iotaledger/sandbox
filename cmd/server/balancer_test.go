package main

import (
	"testing"
	"time"
)

func TestBalancer(t *testing.T) {
	b := NewIRIBalancer([]string{"http://localhost:11111", "http://localhost:22222", "http://localhost:33333"})
	b.Start(time.Microsecond)
	time.Sleep(time.Millisecond * 500)
	_, err := b.Get()
	if err == nil {
		t.Fatalf("expected to find no available nodes but got one instead")
	}
}
