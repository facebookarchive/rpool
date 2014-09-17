// Package rpool provides a resource pool.
package rpool

import (
	"container/list"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/facebookgo/stats"
)

var (
	errPoolClosed  = errors.New("rpool: pool has been closed")
	errCloseAgain  = errors.New("rpool: Pool.Close called more than once")
	closedSentinel = sentinelCloser(1)
	newSentinel    = sentinelCloser(2)
)

// Pool manages the life cycle of resources.
type Pool struct {
	// New is used to create a new resource when necessary.
	New func() (io.Closer, error)

	// CloseErrorHandler will be called when an error occurs closing a resource.
	CloseErrorHandler func(err error)

	// Stats is optional and allows for the pool to provide stats for various
	// interesting events in the proxy.
	Stats stats.Client

	// Max defines the maximum number of concurrently allocated resources.
	Max uint

	// MinIdle defines the number of minimum idle resources. These number of
	// resources are kept around when the idle cleanup kicks in.
	MinIdle uint

	// IdleTimeout defines the duration of idle time after which a resource will
	// be closed.
	IdleTimeout time.Duration

	// ClosePoolSize defines the number of concurrent goroutines that will close
	// resources.
	ClosePoolSize uint

	manageOnce sync.Once
	acquire    chan chan io.Closer
	release    chan io.Closer
	discard    chan struct{}
	close      chan chan error
	closers    chan io.Closer
}

// Acquire will pull a resource from the pool or create a new one if necessary.
func (p *Pool) Acquire() (io.Closer, error) {
	defer stats.BumpTime(p.Stats, "acquire.time").End()
	p.manageOnce.Do(p.goManage)
	r := make(chan io.Closer)
	p.acquire <- r
	c := <-r

	// sentinel value indicates the pool is closed
	if c == closedSentinel {
		return nil, errPoolClosed
	}

	// need to allocate a new resource
	if c == newSentinel {
		t := stats.BumpTime(p.Stats, "acquire.new.time")
		c, err := p.New()
		t.End()
		stats.BumpSum(p.Stats, "acquire.new", 1)
		if err != nil {
			stats.BumpSum(p.Stats, "acquire.error.new", 1)
			// discard our assumed checked out resource since we failed to New
			p.discard <- struct{}{}
		}
		return c, err
	}

	// successfully acquired from pool
	return c, nil
}

// Release puts the resource back into the pool.
func (p *Pool) Release(c io.Closer) {
	p.manageOnce.Do(p.goManage)
	p.release <- c
}

// Discard closes the resource and indicates we're throwing it away.
func (p *Pool) Discard(c io.Closer) {
	p.manageOnce.Do(p.goManage)
	p.closers <- c
	p.discard <- struct{}{}
}

// Close closes the pool and its resources. It waits until all acquired
// resources are released or discarded. It is an error to call Acquire after
// closing the pool.
func (p *Pool) Close() error {
	defer stats.BumpTime(p.Stats, "shutdown.time").End()
	p.manageOnce.Do(p.goManage)
	r := make(chan error)
	p.close <- r
	return <-r
}

func (p *Pool) goManage() {
	if p.Max == 0 {
		panic("no max configured")
	}
	if p.IdleTimeout.Nanoseconds() == 0 {
		panic("no idle timeout configured")
	}
	if p.ClosePoolSize == 0 {
		panic("no close pool size configured")
	}

	p.release = make(chan io.Closer)
	p.acquire = make(chan chan io.Closer)
	p.discard = make(chan struct{})
	p.close = make(chan chan error)
	p.closers = make(chan io.Closer, p.Max)
	go p.manage()
}

type entry struct {
	resource io.Closer
	use      time.Time
}

func (p *Pool) manage() {
	// setup goroutines to close resources
	var closeWG sync.WaitGroup
	closeWG.Add(int(p.ClosePoolSize))
	for i := uint(0); i < p.ClosePoolSize; i++ {
		go func() {
			defer closeWG.Done()
			for c := range p.closers {
				t := stats.BumpTime(p.Stats, "close.time")
				stats.BumpSum(p.Stats, "close", 1)
				if err := c.Close(); err != nil {
					stats.BumpSum(p.Stats, "close.error", 1)
					p.CloseErrorHandler(err)
				}
				t.End()
			}
		}()
	}

	// setup a ticker to report various averages every minute. if we don't have a
	// Stats implementation provided, we Stop it so it never ticks.
	statsTicker := time.NewTicker(time.Minute)
	if p.Stats == nil {
		statsTicker.Stop()
	}

	resources := []entry{}
	out := uint(0)
	waiting := list.New()
	idleTicker := time.NewTicker(p.IdleTimeout)
	closed := false
	var closeResponse chan error
	for {
		if closed && out == 0 && waiting.Len() == 0 {
			if p.Stats != nil {
				statsTicker.Stop()
			}

			// all waiting acquires are done, all resources have been released.
			// now just wait for all resources to close.
			close(p.closers)
			closeWG.Wait()

			// close internal channels.
			close(p.acquire)
			close(p.release)
			close(p.discard)
			close(p.close)

			// return a response to the original close.
			closeResponse <- nil

			return
		}

		select {
		case r := <-p.acquire:
			// if closed, new acquire calls are rejected
			if closed {
				r <- closedSentinel
				stats.BumpSum(p.Stats, "acquire.error.closed", 1)
				continue
			}

			// acquire from pool
			if cl := len(resources); cl > 0 {
				c := resources[cl-1]
				r <- c.resource
				resources = resources[:cl-1]
				out++
				stats.BumpSum(p.Stats, "acquire.pool", 1)
				continue
			}

			// max resources already in use, need to block & wait
			if out == p.Max {
				waiting.PushBack(r)
				stats.BumpSum(p.Stats, "acquire.waiting", 1)
				continue
			}

			// Make a new resource in the calling goroutine by sending it a
			// newSentinel. We assume it's checked out. Acquire will discard if
			// creating a new resource fails.
			out++
			r <- newSentinel
		case c := <-p.release:
			// pass it to someone who's waiting
			if e := waiting.Front(); e != nil {
				r := waiting.Remove(e).(chan io.Closer)
				r <- c
				continue
			}

			if out == 0 {
				panic("releasing more than acquired")
			}

			// no longer out
			out--

			// no one is waiting, and we're closed, schedule it to be closed
			if closed {
				p.closers <- c
				continue
			}

			// put it back in our pool
			resources = append(resources, entry{resource: c, use: time.Now()})
		case <-p.discard:
			// we can make a new one if someone is waiting. no need to decrement out
			// in this case since we assume this new one is checked out. Acquire will
			// discard if creating a new resource fails.
			if e := waiting.Front(); e != nil {
				r := waiting.Remove(e).(chan io.Closer)
				r <- newSentinel
				continue
			}

			if out == 0 {
				panic("discarding more than acquired")
			}

			// otherwise we lost a resource and dont need a new one right away
			out--
		case now := <-idleTicker.C:
			eligibleOffset := len(resources) - int(p.MinIdle)

			// less than min idle, nothing to do
			if eligibleOffset <= 0 {
				continue
			}

			t := stats.BumpTime(p.Stats, "idle.cleanup.time")

			// cleanup idle resources
			idleLen := 0
			for _, e := range resources[:eligibleOffset] {
				if now.Sub(e.use) < p.IdleTimeout {
					break
				}
				p.closers <- e.resource
				idleLen++
			}

			// move the remaining resources to the beginning
			resources = resources[:copy(resources, resources[idleLen:])]

			t.End()
			stats.BumpSum(p.Stats, "idle.closed", float64(idleLen))
		case <-statsTicker.C:
			// We can assume if we hit this then p.Stats is not nil
			p.Stats.BumpAvg("waiting", float64(waiting.Len()))
			p.Stats.BumpAvg("idle", float64(len(resources)))
			p.Stats.BumpAvg("out", float64(out))
			p.Stats.BumpAvg("alive", float64(uint(len(resources))+out))
		case r := <-p.close:
			// cant call close if already closing
			if closed {
				r <- errCloseAgain
				continue
			}

			closed = true
			idleTicker.Stop() // stop idle processing

			// close idle since if we have idle, implicitly no one is waiting
			for _, e := range resources {
				p.closers <- e.resource
			}

			closeResponse = r
		}
	}
}

type sentinelCloser int

func (s sentinelCloser) Close() error {
	panic("should never get called")
}
