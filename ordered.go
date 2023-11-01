// Copyright (c) technicianted. All rights reserved.
// Licensed under the MIT License.
package main

import (
	"context"
	"sync"

	"github.com/technicianted/bloque"
	"go.uber.org/atomic"
)

type workItem[R any] struct {
	index uint64
	f     func() R
}

type workItemResult[R any] struct {
	index  uint64
	result R
}

type OrderedScatterGatherer[R any] struct {
	workQueue            *bloque.Bloque
	outputQueue          *bloque.Bloque
	relayCTXCancel       context.CancelFunc
	workersWG            sync.WaitGroup
	scatterChan          chan workItem[R]
	gatherChan           chan workItemResult[R]
	resultsChan          chan R
	gatherDoneChan       chan struct{}
	pickWorkDoneChan     chan struct{}
	relayResultsDoneChan chan struct{}
	lastIndex            atomic.Uint64
}

// Create a new ordered scatter gatherer with workers go routines and maxInputBacklog
// for input work items, and maxOutputBacklog for output results.
// Implementation guarantees results will be consumed in the same order work
// is added.
// A value of 0 for either maxInputBacklog or maxOutputBacklog means unbound.
// Workers are started immediately. A call to Drain() is required to stop and
// cleanup.
func NewOrderedScatterGather[R any](workers int, maxInputBacklog int, maxOutputBacklog int) *OrderedScatterGatherer[R] {
	sg := &OrderedScatterGatherer[R]{
		workQueue:            bloque.New(bloque.WithCapacity(maxInputBacklog)),
		outputQueue:          bloque.New(bloque.WithCapacity(maxOutputBacklog)),
		scatterChan:          make(chan workItem[R]),
		gatherChan:           make(chan workItemResult[R]),
		resultsChan:          make(chan R),
		pickWorkDoneChan:     make(chan struct{}),
		gatherDoneChan:       make(chan struct{}),
		relayResultsDoneChan: make(chan struct{}),
	}

	for i := 0; i < workers; i++ {
		sg.workersWG.Add(1)
		go func() {
			sg.doWork()
			sg.workersWG.Done()
		}()
	}

	ctx, cancel := context.WithCancel(context.Background())
	sg.relayCTXCancel = cancel
	go sg.pickWork(ctx)
	go sg.gatherWork(ctx)
	go sg.relayResults(ctx)

	return sg
}

// Return a read-only channel to consume results. Results are guaranteed to
// be read in the same order work was added.
func (sg *OrderedScatterGatherer[R]) OutputChannel() <-chan R {
	return sg.resultsChan
}

// Add new work done by function f. Implementation guarantees that results will
// be read in the same order work was added.
// If maxInputBacklog was specified and exceeded, call will block and may be
// cancelled by cancelling ctx.
func (sg *OrderedScatterGatherer[R]) AddWork(ctx context.Context, f func() R) error {
	index := sg.lastIndex.Add(1)
	return sg.workQueue.Push(ctx, workItem[R]{
		index: index,
		f:     f,
	})
}

// Wait for all work to completed and cleanup all resources.
func (sg *OrderedScatterGatherer[R]) Drain() error {
	sg.workQueue.Close()
	<-sg.pickWorkDoneChan
	close(sg.scatterChan)
	sg.workersWG.Wait()
	close(sg.gatherChan)
	<-sg.gatherDoneChan
	sg.outputQueue.Close()
	<-sg.relayResultsDoneChan
	close(sg.resultsChan)

	return nil
}

func (sg *OrderedScatterGatherer[R]) doWork() {
	for item := range sg.scatterChan {
		r := item.f()
		sg.gatherChan <- workItemResult[R]{
			index:  item.index,
			result: r,
		}
	}
}

func (sg *OrderedScatterGatherer[R]) pickWork(ctx context.Context) {
	for {
		item, err := sg.workQueue.Pop(ctx)
		if err != nil {
			break
		}
		sg.scatterChan <- item.(workItem[R])
	}

	close(sg.pickWorkDoneChan)
}

func (sg *OrderedScatterGatherer[R]) relayResults(ctx context.Context) {
	for {
		result, err := sg.outputQueue.Pop(ctx)
		if err != nil {
			break
		}
		sg.resultsChan <- result.(R)
	}

	close(sg.relayResultsDoneChan)
}

func (sg *OrderedScatterGatherer[R]) gatherWork(ctx context.Context) {
	bufferedItems := map[uint64]workItemResult[R]{}
	awaitingIndex := uint64(1)
	for result := range sg.gatherChan {
		bufferedItems[result.index] = result
		for {
			bufferedItem, ok := bufferedItems[awaitingIndex]
			if !ok {
				break
			}
			err := sg.outputQueue.Push(ctx, bufferedItem.result)
			if err != nil {
				break
			}
			delete(bufferedItems, awaitingIndex)
			awaitingIndex++
		}
	}

	close(sg.gatherDoneChan)
}
