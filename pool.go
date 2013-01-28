// Modified from https://github.com/garyburd/redigo/blob/master/redis/pool.go
// Copyright 2012 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package dilithium

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

var nowFunc = time.Now // for testing

var errPoolClosed = errors.New("dilithium: connection pool closed")

// Pool maintains a pool of connections. The application calls the Get method
// to get a connection from the pool and the connection's Close method to
// return the connection's resources to the pool.
type Pool struct {
	// Dial is an application supplied function for creating new connections.
	Dial func(url string) (Closer, error)

	// TestOnBorrow is an optional application supplied function for checking
	// the health of an idle connection before the connection is used again by
	// the application. Argument t is the time that the connection was returned
	// to the pool. If the function returns an error, then the connection is
	// closed.
	TestOnBorrow func(c Closer, t time.Time) error

	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	// url is the connection string used by Dial, typically a URL
	url string

	// mu protects fields defined below.
	mu     sync.Mutex
	closed bool

	// Stack of idleConn with most recently used at the front.
	idle list.List
}

type Closer interface {
	Close()
}

type idleConn struct {
	c Closer
	t time.Time
}

// NewPool returns a pool that uses newPool to create connections as needed.
// The pool keeps a maximum of maxIdle idle connections.
func NewPool(newFn func(string) (Closer, error), maxIdle int) *Pool {
	return &Pool{Dial: newFn, MaxIdle: maxIdle}
}

// Get gets a connection from the pool.
func (p *Pool) Get() (*pooledConnection, error) {
	c := &pooledConnection{p: p}
	err := c.get()
	return c, err
}

// Close releases the resources used by the pool.
func (p *Pool) Close() {
	p.mu.Lock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(idleConn).c.Close()
	}
}

// get prunes stale connections and returns a connection from the idle list or
// creates a new connection.
func (p *Pool) get() (Closer, error) {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("dilithium: get on closed pool")
	}

	// Prune stale connections.
	if timeout := p.IdleTimeout; timeout > 0 {
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Back()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			if ic.t.Add(timeout).After(nowFunc()) {
				break
			}
			p.idle.Remove(e)
			p.mu.Unlock()
			ic.c.Close()
			p.mu.Lock()
		}
	}

	// Get idle connection.
	for i, n := 0, p.idle.Len(); i < n; i++ {
		e := p.idle.Front()
		if e == nil {
			break
		}
		ic := e.Value.(idleConn)
		p.idle.Remove(e)
		test := p.TestOnBorrow
		p.mu.Unlock()
		if test != nil && test(ic.c, ic.t) != nil {
			ic.c.Close()
		} else {
			return ic.c, nil
		}
		p.mu.Lock()
	}

	// No idle connection, create new.
	dial := p.Dial
	p.mu.Unlock()
	return dial(p.url)
}

func (p *Pool) put(c Closer) {
	p.mu.Lock()
	if !p.closed {
		p.idle.PushFront(idleConn{t: nowFunc(), c: c})
		if p.idle.Len() > p.MaxIdle {
			c = p.idle.Remove(p.idle.Back()).(idleConn).c
		} else {
			c = nil
		}
	}
	p.mu.Unlock()
	if c != nil {
		c.Close()
	}
}

type pooledConnection struct {
	c   Closer
	err error
	p   *Pool
}

func (c *pooledConnection) get() error {
	if c.err == nil && c.c == nil {
		c.c, c.err = c.p.get()
	}
	return c.err
}

func (c *pooledConnection) Close() {
	if c.c != nil {
		c.p.put(c.c)
		c.c = nil
		c.err = errPoolClosed
	}
}

func (c *pooledConnection) Err() error {
	if err := c.get(); err != nil {
		return err
	}
	return nil
}

var (
	poolTypes    = make(map[string]*Pool)
	poolTypesMtx sync.RWMutex
)

func RegisterPoolType(name string, pool *Pool) {
	poolTypesMtx.Lock()
	defer poolTypesMtx.Unlock()
	poolTypes[name] = pool
}
