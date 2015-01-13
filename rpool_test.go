package rpool_test

import (
	"errors"
	"io"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/facebookgo/clock"
	"github.com/facebookgo/ensure"
	"github.com/facebookgo/rpool"
	"github.com/facebookgo/stats"
)

var errWrongPool = errors.New("rpool: provided resource was not acquired from this pool")

type resource struct {
	resourceMaker *resourceMaker
}

func (r *resource) Close() error {
	atomic.AddInt32(&r.resourceMaker.closeCount, 1)
	return nil
}

type resourceMaker struct {
	newCount   int32
	closeCount int32
}

func (r *resourceMaker) New() (io.Closer, error) {
	atomic.AddInt32(&r.newCount, 1)
	return &resource{resourceMaker: r}, nil
}

type resourceCloseErr struct {
	err error
}

func (r *resourceCloseErr) Close() error {
	return r.err
}

func TestAcquireMakesANewResource(t *testing.T) {
	t.Parallel()
	var cm resourceMaker
	p := rpool.Pool{
		New:           cm.New,
		Max:           1,
		MinIdle:       1,
		IdleTimeout:   time.Hour,
		ClosePoolSize: 1,
	}
	r, err := p.Acquire()
	ensure.Nil(t, err)
	p.Release(r)
	ensure.Nil(t, p.Close())
	ensure.DeepEqual(t, atomic.LoadInt32(&cm.newCount), int32(1))
	ensure.DeepEqual(t, atomic.LoadInt32(&cm.closeCount), int32(1))
}

func TestMax(t *testing.T) {
	t.Parallel()

	// we going to wait until stats shows we have 1 waiting acquire
	statsDone := make(chan struct{})
	doneWaiting := false
	hc := &stats.HookClient{
		BumpSumHook: func(key string, val float64) {
			if !doneWaiting && key == "acquire.waiting" && val == 1 {
				doneWaiting = true
				close(statsDone)
			}
		},
	}

	var cm resourceMaker
	p := rpool.Pool{
		New:           cm.New,
		Stats:         hc,
		Max:           1,
		MinIdle:       1,
		IdleTimeout:   time.Hour,
		ClosePoolSize: 2,
	}

	// acquire and hold max
	r, err := p.Acquire()
	ensure.Nil(t, err)

	// try to acquire again blocks
	done := make(chan struct{})
	go func() {
		defer close(done)
		r, err := p.Acquire()
		ensure.Nil(t, err)
		p.Release(r)
	}()

	// stats should soon show one waiting
	<-statsDone

	// release the first one
	p.Release(r)

	ensure.Nil(t, p.Close())
	<-done
	ensure.DeepEqual(t, atomic.LoadInt32(&cm.newCount), int32(1))
	ensure.DeepEqual(t, atomic.LoadInt32(&cm.closeCount), int32(1))
}

func TestUseIdle(t *testing.T) {
	t.Parallel()

	// expected stats
	statsDone := make(chan struct{})
	hc := &stats.HookClient{
		BumpSumHook: func(key string, val float64) {
			if key == "acquire.pool" {
				close(statsDone)
			}
		},
	}

	var cm resourceMaker
	p := rpool.Pool{
		New:           cm.New,
		Stats:         hc,
		Max:           1,
		MinIdle:       1,
		IdleTimeout:   time.Hour,
		ClosePoolSize: 2,
	}

	// acquire and release
	r1, err := p.Acquire()
	ensure.Nil(t, err)
	p.Release(r1)

	// acquire should use the existing one
	r2, err := p.Acquire()
	ensure.Nil(t, err)

	p.Release(r2)

	<-statsDone

	ensure.Nil(t, p.Close())
	ensure.DeepEqual(t, atomic.LoadInt32(&cm.newCount), int32(1))
	ensure.DeepEqual(t, atomic.LoadInt32(&cm.closeCount), int32(1))
}

func TestNewError(t *testing.T) {
	t.Parallel()

	// expected stats
	statsDone := make(chan struct{})
	hc := &stats.HookClient{
		BumpSumHook: func(key string, val float64) {
			if key == "acquire.error.new" {
				close(statsDone)
			}
		},
	}

	expected := errors.New("")
	p := rpool.Pool{
		New:           func() (io.Closer, error) { return nil, expected },
		Stats:         hc,
		Max:           1,
		MinIdle:       1,
		IdleTimeout:   time.Hour,
		ClosePoolSize: 2,
	}
	r, actual := p.Acquire()
	ensure.Nil(t, r)
	ensure.DeepEqual(t, actual, expected)
	ensure.Nil(t, p.Close())
	<-statsDone
}

func TestAcquireAfterClose(t *testing.T) {
	t.Parallel()

	// expected stats
	statsDone := make(chan struct{})
	hc := &stats.HookClient{
		BumpSumHook: func(key string, val float64) {
			if key == "acquire.error.closed" {
				close(statsDone)
			}
		},
	}

	var cm resourceMaker
	p := rpool.Pool{
		New:           cm.New,
		Stats:         hc,
		Max:           1,
		MinIdle:       1,
		IdleTimeout:   time.Hour,
		ClosePoolSize: 2,
	}

	// acquire and hold max
	r1, err := p.Acquire()
	ensure.Nil(t, err)

	// close in two goroutines, one of them should error
	closeError := make(chan error, 1)
	closeDone1 := make(chan struct{})
	closeDone2 := make(chan struct{})
	go func() {
		defer close(closeDone1)
		if err := p.Close(); err != nil {
			closeError <- err
		}
	}()
	go func() {
		defer close(closeDone2)
		if err := p.Close(); err != nil {
			closeError <- err
		}
	}()
	ensure.Err(t, <-closeError, regexp.MustCompile("Pool.Close called more than once"))

	// acquire now fails
	r2, err := p.Acquire()
	ensure.Nil(t, r2)
	ensure.Err(t, err, regexp.MustCompile("pool has been closed"))

	p.Release(r1)

	// both close should be done now
	<-closeDone1
	<-closeDone2

	<-statsDone

	ensure.DeepEqual(t, atomic.LoadInt32(&cm.newCount), int32(1))
	ensure.DeepEqual(t, atomic.LoadInt32(&cm.closeCount), int32(1))
}

func TestMaxAndDiscard(t *testing.T) {
	t.Parallel()

	// we going to wait until stats shows we have 1 waiting acquire
	statsDone := make(chan struct{})
	doneWaiting := false
	hc := &stats.HookClient{
		BumpSumHook: func(key string, val float64) {
			if !doneWaiting && key == "acquire.waiting" && val == 1 {
				doneWaiting = true
				close(statsDone)
			}
		},
	}

	var cm resourceMaker
	p := rpool.Pool{
		New:           cm.New,
		Stats:         hc,
		Max:           1,
		MinIdle:       1,
		IdleTimeout:   time.Hour,
		ClosePoolSize: 2,
	}

	// acquire and hold max
	r, err := p.Acquire()
	ensure.Nil(t, err)

	// try to acquire again blocks
	done := make(chan struct{})
	go func() {
		defer close(done)
		r, err := p.Acquire()
		ensure.Nil(t, err)
		p.Release(r)
	}()

	// stats should soon show one waiting
	<-statsDone

	// discard the first one
	p.Discard(r)

	ensure.Nil(t, p.Close())
	<-done
	ensure.DeepEqual(t, atomic.LoadInt32(&cm.newCount), int32(2))
	ensure.DeepEqual(t, atomic.LoadInt32(&cm.closeCount), int32(2))
}

func TestCloseErrorHandler(t *testing.T) {
	t.Parallel()

	// expected stats
	statsDone := make(chan struct{})
	hc := &stats.HookClient{
		BumpSumHook: func(key string, val float64) {
			if key == "close.error" {
				close(statsDone)
			}
		},
	}

	done := make(chan struct{})
	expected := errors.New("")
	p := rpool.Pool{
		New: func() (io.Closer, error) {
			return &resourceCloseErr{err: expected}, nil
		},
		CloseErrorHandler: func(actual error) {
			defer close(done)
			ensure.DeepEqual(t, actual, expected)
		},
		Stats:         hc,
		Max:           1,
		MinIdle:       1,
		IdleTimeout:   time.Hour,
		ClosePoolSize: 1,
	}
	r, err := p.Acquire()
	ensure.Nil(t, err)
	p.Release(r)
	ensure.Nil(t, p.Close())
	<-done
	<-statsDone
}

func TestDoublePoolClose(t *testing.T) {
	t.Parallel()
	var cm resourceMaker
	p := rpool.Pool{
		New:           cm.New,
		Max:           1,
		MinIdle:       1,
		IdleTimeout:   time.Hour,
		ClosePoolSize: 2,
	}

	// acquire and hold
	r1, err := p.Acquire()
	ensure.Nil(t, err)

	// close in two goroutines, one of them should error
	closeError := make(chan error, 1)
	closeDone1 := make(chan struct{})
	closeDone2 := make(chan struct{})
	go func() {
		defer close(closeDone1)
		if err := p.Close(); err != nil {
			closeError <- err
		}
	}()
	go func() {
		defer close(closeDone2)
		if err := p.Close(); err != nil {
			closeError <- err
		}
	}()
	ensure.Err(t, <-closeError, regexp.MustCompile("Pool.Close called more than once"))

	// release original resource
	p.Release(r1)

	// both close should be done now
	<-closeDone1
	<-closeDone2

	ensure.DeepEqual(t, atomic.LoadInt32(&cm.newCount), int32(1))
	ensure.DeepEqual(t, atomic.LoadInt32(&cm.closeCount), int32(1))
}

func TestIdleClose(t *testing.T) {
	t.Parallel()

	const (
		max     = 5
		minIdle = 2
	)

	klock := clock.NewMock()

	// we going to wait until stats shows we closed enough idle
	statsDone := make(chan struct{})
	expectedIdleClosed := int32(max - minIdle)
	hc := &stats.HookClient{
		BumpSumHook: func(key string, val float64) {
			if key == "idle.closed" {
				if atomic.AddInt32(&expectedIdleClosed, -int32(val)) == 0 {
					close(statsDone)
				}
			}
		},
	}

	var cm resourceMaker
	p := rpool.Pool{
		New:           cm.New,
		Stats:         hc,
		Max:           max,
		MinIdle:       minIdle,
		IdleTimeout:   time.Second,
		ClosePoolSize: 2,
		Clock:         klock,
	}

	// acquire and release some resources to make them idle
	var resources []io.Closer
	for i := max; i > 0; i-- {
		r, err := p.Acquire()
		ensure.Nil(t, err)
		resources = append(resources, r)
	}

	// this will be used as the "last use" time
	klock.Add(p.IdleTimeout / 2)

	for _, r := range resources {
		p.Release(r)
	}

	// tick another IdleTimeout / 2 to hit the 0 eligible case
	klock.Add(p.IdleTimeout / 2)

	// tick another IdleTimeout to make them eligible
	klock.Add(p.IdleTimeout)

	// stats should soon show idle closed
	<-statsDone

	// tick another IdleTimeout to hit the eligibleOffset check
	klock.Add(p.IdleTimeout)

	ensure.Nil(t, p.Close())
	ensure.DeepEqual(t, atomic.LoadInt32(&cm.newCount), int32(max))
	ensure.DeepEqual(t, atomic.LoadInt32(&cm.closeCount), int32(max))
}

func TestReleaseInvalid(t *testing.T) {
	t.Parallel()
	defer ensure.PanicDeepEqual(t, errWrongPool)
	var cm resourceMaker
	p := rpool.Pool{
		New:           cm.New,
		Max:           1,
		MinIdle:       1,
		IdleTimeout:   time.Hour,
		ClosePoolSize: 1,
	}
	r, err := cm.New()
	ensure.Nil(t, err)
	p.Release(r)
}

func TestDiscardInvalid(t *testing.T) {
	t.Parallel()
	defer ensure.PanicDeepEqual(t, errWrongPool)
	var cm resourceMaker
	p := rpool.Pool{
		New:           cm.New,
		Max:           1,
		MinIdle:       1,
		IdleTimeout:   time.Hour,
		ClosePoolSize: 1,
	}
	r, err := cm.New()
	ensure.Nil(t, err)
	p.Discard(r)
}

func TestMaxUndefined(t *testing.T) {
	t.Parallel()
	defer ensure.PanicDeepEqual(t, "no max configured")
	(&rpool.Pool{}).Acquire()
}

func TestIdleTimeoutUndefined(t *testing.T) {
	t.Parallel()
	defer ensure.PanicDeepEqual(t, "no idle timeout configured")
	(&rpool.Pool{Max: 1}).Acquire()
}

func TestClosePoolSizeUndefined(t *testing.T) {
	t.Parallel()
	defer ensure.PanicDeepEqual(t, "no close pool size configured")
	(&rpool.Pool{
		Max:         1,
		IdleTimeout: time.Second,
	}).Acquire()
}

func TestStatsTicker(t *testing.T) {
	t.Parallel()

	klock := clock.NewMock()
	expected := []string{"alive", "idle", "out", "waiting"}

	statsDone := make(chan string, 4)
	hc := &stats.HookClient{
		BumpAvgHook: func(key string, val float64) {
			if contains(expected, key) {
				statsDone <- key
			}
		},
	}

	var cm resourceMaker
	p := rpool.Pool{
		New:           cm.New,
		Stats:         hc,
		Max:           4,
		MinIdle:       2,
		IdleTimeout:   time.Second,
		ClosePoolSize: 2,
		Clock:         klock,
	}

	// acquire and release some resources to make them idle
	var resources []io.Closer
	for i := p.Max; i > 0; i-- {
		r, err := p.Acquire()
		ensure.Nil(t, err)
		resources = append(resources, r)
	}
	for _, r := range resources {
		p.Release(r)
	}

	// tick IdleTimeout to make them eligible
	klock.Add(p.IdleTimeout)

	// tick Minute to trigger stats
	klock.Add(time.Minute)

	// stats should soon show idle closed
	ensure.SameElements(
		t,
		[]string{<-statsDone, <-statsDone, <-statsDone, <-statsDone},
		expected,
	)
}

func contains(set []string, key string) bool {
	for _, v := range set {
		if v == key {
			return true
		}
	}
	return false
}
