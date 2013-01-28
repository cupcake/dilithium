// Derived from the Go net/rpc standard library.
// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package dilithium

import (
	"errors"
	"log"
	"net/rpc"
	"reflect"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

type methodType struct {
	method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
	readOnly  bool // if false, ReplyType is nil
}

type service struct {
	name    string
	rcvr    reflect.Value
	typ     reflect.Type
	methods map[string]*methodType
}

type Server struct {
	forwarding *ForwardingTable
	mu         sync.RWMutex // protects services
	services   map[string]*service
	queryLock  sync.Mutex
	nextQuery  *Query
}

type rpcServer Server

// Precompute the reflect type for error.  Can't use error directly
// because Typeof takes an empty interface value.  This is annoying.
var typeOfError = reflect.TypeOf((*error)(nil)).Elem()
var typeOfQueryArg = reflect.TypeOf((*QueryArg)(nil)).Elem()

var DefaultServer = NewServer(nil)

func NewServer(forwarding *ForwardingTable) *Server {
	if forwarding == nil {
		forwarding = &ForwardingTable{}
	}
	return &Server{forwarding: forwarding, services: make(map[string]*service)}
}

func isExported(name string) bool {
	r, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(r)
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return isExported(t.Name()) || t.PkgPath() == ""
}

func (s *Server) Register(rcvr interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	service := &service{typ: reflect.TypeOf(rcvr), rcvr: reflect.ValueOf(rcvr)}
	name := reflect.Indirect(service.rcvr).Type().Name()
	if name == "" {
		log.Fatal("dilithium: no service name for type", service.typ.String())
	}
	if !isExported(name) {
		e := "dilithium Register: type " + name + " is not exported"
		log.Fatal(s)
		return errors.New(e)
	}
	if _, present := s.services[name]; present {
		return errors.New("dilithium: service already defined: " + name)
	}
	service.name = name
	service.methods = make(map[string]*methodType)

	for i := 0; i < service.typ.NumMethod(); i++ {
		method := service.typ.Method(i)
		mtype := method.Type
		mname := method.Name
		// Method must be exported
		if method.PkgPath != "" {
			continue
		}
		// Method needs three or four ins.
		// ReadOnly: receiver, *conn, *arg, *reply
		// Write: receiver, *conn, *arg
		if mtype.NumIn() != 3 && mtype.NumIn() != 4 {
			log.Println("method", mname, "has wrong number of ins:", mtype.NumIn())
			continue
		}
		// First arg must be a pointer.
		connType := mtype.In(1)
		if connType.Kind() != reflect.Ptr {
			log.Println("method", mname, "connection type not a pointer:", connType)
			continue
		}
		// Connection type must be exported.
		if !isExportedOrBuiltinType(connType) {
			log.Println("method", mname, "connection type not exported:", connType)
			continue
		}
		// Second arg need not be a pointer, but must be exported and implement QueryArg.
		argType := mtype.In(2)
		if !isExportedOrBuiltinType(argType) {
			log.Println(mname, "argument type not exported:", argType)
			continue
		}
		if !argType.Implements(typeOfQueryArg) {
			log.Println(mname, "argument type does not implement QueryArg:", argType)
			continue
		}
		var replyType reflect.Type
		var readOnly bool
		if mtype.NumIn() == 4 {
			readOnly = true
			// Third arg must be a pointer.
			replyType = mtype.In(3)
			if replyType.Kind() != reflect.Ptr {
				log.Println("method", mname, "reply type not a pointer:", replyType)
				continue
			}
			// Reply type must be exported.
			if !isExportedOrBuiltinType(replyType) {
				log.Println("method", mname, "reply type not exported:", replyType)
				continue
			}
		}
		// Method needs one out.
		if mtype.NumOut() != 1 {
			log.Println("method", mname, "has wrong number of outs:", mtype.NumOut())
			continue
		}
		// The return type of the method must be error.
		if returnType := mtype.Out(0); returnType != typeOfError {
			log.Println("method", mname, "returns", returnType.String(), "not error")
			continue
		}
		service.methods[mname] = &methodType{method: method, ArgType: argType, ReplyType: replyType.Elem(), readOnly: readOnly}
	}

	if len(service.methods) == 0 {
		e := "dilithium Register: type " + name + " has no exported methods of suitable type"
		log.Fatal(e)
		return errors.New(e)
	}
	s.services[service.name] = service
	return nil
}

func (s *Server) RegisterWithRPC(r *rpc.Server) {
	server := rpcServer(*s)
	r.RegisterName("dilithium", &server)
}

func (s *rpcServer) Query(q *Query, reply *interface{}) error {
	q.server = s
	serviceMethod := strings.Split(q.Method, ".")
	if len(serviceMethod) != 2 {
		return errors.New("dilithium: query service/method invalid: " + q.Method)
	}
	s.mu.RLock()
	q.service = s.services[serviceMethod[0]]
	s.mu.RUnlock()
	if q.service == nil {
		return errors.New("dilithium: can't find service " + serviceMethod[0])
	}
	q.method = q.service.methods[serviceMethod[1]]
	if q.method == nil {
		return errors.New("dilithium: can't find method " + q.Method)
	}

	err := q.Route()
	if err != nil {
		return err
	}
	*reply = q.Reply
	return nil
}
