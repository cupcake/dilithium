package dilithium_test

import (
	"encoding/gob"
	"net"
	"net/rpc"
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

func (d *ExampleDatastore) Dial(url string) {
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

type IntShardKey int

func (k IntShardKey) ShardKey() int { return int(k) }

type ExampleService struct{}

func (s *ExampleService) GetURL(conn *ExampleDatastore, k IntShardKey, url *string) error {
	*url = conn.Get("url")
	return nil
}

var client *rpc.Client
var forwardingTable *dilithium.ForwardingTable

func startServer() {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	rpcServer := rpc.NewServer()

	forwardingTable = &dilithium.ForwardingTable{}
	shard := &dilithium.PhysicalShard{}
	shard.Setup(map[string]interface{}{"url": "shard1", "pool": "example"})
	forwardingTable.Insert(&dilithium.ForwardingTableEntry{100, shard})

	dserver := dilithium.NewServer(forwardingTable)
	dserver.Register(&ExampleService{})
	dserver.RegisterWithRPC(rpcServer)

	go rpcServer.Accept(l)
	client, err = rpc.Dial("tcp", l.Addr().String())
	if err != nil {
		panic(err)
	}
}

func (s *RPCSuite) SetUpSuite(c *C) {
	startServer()
}

func (s *RPCSuite) TestStuff(c *C) {
	res := new(interface{})
	err := client.Call("dilithium.Query", &dilithium.Query{Method: "ExampleService.GetURL", Arg: IntShardKey(1)}, res)
	MaybeFail(c, err)
	c.Assert(*res, Equals, "shard1")
}

func MaybeFail(c *C, err error) {
	if err != nil {
		c.Log(err)
		c.FailNow()
	}
}

func init() {
	dilithium.RegisterPoolType("example", &dilithium.Pool{
		Dial: func(url string) (dilithium.Closer, error) {
			c := &ExampleDatastore{}
			c.Dial(url)
			return c, nil
		},
	})
	gob.Register(IntShardKey(0))
}
