package dilithium_test

import (
	"sync"
	"testing"

	"cupcake.io/dilithium"
	. "launchpad.net/gocheck"
)

// Hook gocheck into the gotest runner.
func Test(t *testing.T) { TestingT(t) }

type RPCSuite struct{}

var _ = Suite(&RPCSuite{})

type ExampleDatastore struct {
	open bool
	sync.RWMutex
	data map[string]string
}

func (d *ExampleDatastore) Open(url string) {
	d.Lock()
	defer d.Unlock()
	if d.open {
		panic("ExampleDatastore already open")
	}
	d.open = true
	d.data = make(map[string]string)
	d.data["url"] = url
}

func (d *ExampleDatastore) Close() {
	d.Lock()
	defer d.Unlock()
	if !d.open {
		panic("ExampleDatastore already closed")
	}
	d.open = false
}

func (d *ExampleDatastore) Set(key, value string) {
	d.Lock()
	defer d.Unlock()
	if !d.open {
		panic("ExampleDatastore not open")
	}
	d.data[key] = value
}

func (d *ExampleDatastore) Get(key string) string {
	d.RLock()
	defer d.RUnlock()
	if !d.open {
		panic("ExampleDatastore not open")
	}
	return d.data[key]
}

type ExampleService struct{}

func (s *RPCSuite) TestStuff(c *C) {
	
}
