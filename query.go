package dilithium

import (
	"fmt"
	"reflect"
)

type QueryArg interface {
	ShardKey() int
}

type Query struct {
	ServiceMethod string
	Arg           interface{}
	Reply         interface{}
	Error         error
	server        *rpcServer
	service       *service
	method        *methodType
}

func (q *Query) ReadOnly() bool {
	return q.method.readOnly
}

func (q *Query) Route() error {
	key := q.Arg.(QueryArg).ShardKey()
	shard := q.server.forwarding.Lookup(key)
	if shard == nil {
		return fmt.Errorf("dilithium: could not find shard for key: %d", key)
	}
	shard.Query(q)
	return nil
}

func (q *Query) Run(conn interface{}) {
	f := q.method.method.Func

	arg := reflect.ValueOf(q.Arg)
	if q.method.ArgType.Kind() == reflect.Ptr {
		if arg.Kind() != reflect.Ptr {
			arg = arg.Addr()
		}
	} else {
		if arg.Kind() == reflect.Ptr {
			arg = arg.Elem()
		}
	}

	var res []reflect.Value
	if q.ReadOnly() {
		res = f.Call([]reflect.Value{q.service.rcvr, reflect.ValueOf(conn), arg, reflect.ValueOf(&q.Reply)})
	} else {
		res = f.Call([]reflect.Value{q.service.rcvr, reflect.ValueOf(conn), arg})
	}
	q.Error = res[0].Interface().(error)
}
