package dilithium

import (
	"fmt"
	"reflect"
)

type QueryArg interface {
	ShardKey() int
}

type Query struct {
	Method  string
	Arg     QueryArg
	Reply   interface{}
	server  *rpcServer
	service *service
	method  *methodType
}

func (q *Query) ReadOnly() bool {
	return q.method.readOnly
}

func (q *Query) Route() error {
	key := q.Arg.ShardKey()
	shard := q.server.forwarding.Lookup(key)
	if shard == nil {
		return fmt.Errorf("dilithium: could not find shard for key: %d", key)
	}
	return shard.Query(q)
}

func (q *Query) Run(conn interface{}) error {
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
		reply := reflect.New(q.method.ReplyType)
		res = f.Call([]reflect.Value{q.service.rcvr, reflect.ValueOf(conn), arg, reply})
		q.Reply = reply.Elem().Interface()
	} else {
		res = f.Call([]reflect.Value{q.service.rcvr, reflect.ValueOf(conn), arg})
	}
	resErr := res[0].Interface()
	if resErr != nil {
		return resErr.(error)
	}
	return nil
}
