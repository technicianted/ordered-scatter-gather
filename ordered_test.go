// Copyright (c) technicianted. All rights reserved.
// Licensed under the MIT License.
package scattergather

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSGSimple(t *testing.T) {
	sg := NewOrderedScatterGather[int](4, 0, 0)
	defer sg.Drain()

	sg.AddWork(context.Background(), func() int {
		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
		return 1
	})
	sg.AddWork(context.Background(), func() int {
		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
		return 2
	})
	sg.AddWork(context.Background(), func() int {
		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
		return 3
	})

	for i := 1; i <= 3; i++ {
		r := <-sg.OutputChannel()
		assert.Equal(t, i, r)
	}
}

func TestSGDrainWithOngoingWork(t *testing.T) {
	sg := NewOrderedScatterGather[int](4, 0, 0)

	sg.AddWork(context.Background(), func() int {
		time.Sleep(200 * time.Millisecond)
		return 1
	})

	doneChan := make(chan struct{})
	st := time.Now()
	go func() {
		sg.Drain()
		assert.InDelta(t, 200*time.Millisecond, time.Since(st), float64(10*time.Millisecond))
		close(doneChan)
	}()
	<-sg.OutputChannel()
	<-doneChan
}

func TestSGDrainWithUnreadWork(t *testing.T) {
	sg := NewOrderedScatterGather[int](4, 0, 0)

	sg.AddWork(context.Background(), func() int {
		return 1
	})

	doneChan := make(chan struct{})
	st := time.Now()
	go func() {
		sg.Drain()
		assert.InDelta(t, 200*time.Millisecond, time.Since(st), float64(10*time.Millisecond))
		close(doneChan)
	}()
	time.Sleep(200 * time.Millisecond)
	<-sg.OutputChannel()
	<-doneChan
}
