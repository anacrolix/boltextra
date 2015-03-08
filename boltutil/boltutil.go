package boltutil

import (
	"github.com/boltdb/bolt"
)

type Node interface {
	Get() []byte
	Put(value []byte) error
	Child([]byte) Node
	bucketForWrite() (bucket, error)
	bucket() bucket
	ForEach(func(k, v []byte) error) error
}

type visitor func(k, v []byte) error

// The interface shared by bolt.Tx and bolt.Bucket.
type bucket interface {
	Bucket(name []byte) *bolt.Bucket
	CreateBucketIfNotExists(key []byte) (*bolt.Bucket, error)
	Get(key []byte) []byte
	Put(key, value []byte) error
	ForEach(func(k, v []byte) error) error
}

type txBucket struct {
	*bolt.Tx
}

func (txBucket) Get([]byte) []byte {
	panic("can't get on tx bucket")
}

func (txBucket) Put(key, value []byte) error {
	panic("can't put on tx bucket")
}

func (txBucket) ForEach(func(k, v []byte) error) error {
	panic("tx bucket only contains buckets")
}

type rootNode struct {
	tx *bolt.Tx
}

type node struct {
	parent Node
	key    []byte
}

func (me rootNode) Child(key []byte) Node {
	return node{me, key}
}

func (me rootNode) Get() []byte {
	panic("can't get on root node")
}

func (me rootNode) Put([]byte) error {
	panic("can't put on root node")
}

func (me rootNode) bucket() bucket {
	return txBucket{me.tx}
}

func (me rootNode) bucketForWrite() (bucket, error) {
	return txBucket{me.tx}, nil
}

func (me rootNode) ForEach(func(k, v []byte) error) error {
	panic("can't foreach root node")
}

func (me node) Child(key []byte) Node {
	return node{me, key}
}

func (me node) Get() []byte {
	b := me.parent.bucket()
	if b == nil {
		return nil
	}
	return b.Get(me.key)
}

func (me node) Put(value []byte) error {
	b, err := me.parent.bucketForWrite()
	if err != nil {
		return err
	}
	return b.Put(me.key, value)
}

func (me node) bucket() bucket {
	b := me.parent.bucket()
	if b == nil {
		return nil
	}
	bp := b.Bucket(me.key)
	if bp == nil {
		return nil
	}
	return bp
}

func (me node) bucketForWrite() (b bucket, err error) {
	b, err = me.parent.bucketForWrite()
	if err != nil {
		return
	}
	return b.CreateBucketIfNotExists(me.key)
}

func (me node) ForEach(fn func(k, v []byte) error) error {
	b := me.bucket()
	if b == nil {
		return nil
	}
	return b.ForEach(fn)
}

func RootNode(tx *bolt.Tx) Node {
	return rootNode{tx}
}

func Path(tx *bolt.Tx, elems ...string) (ret Node) {
	ret = RootNode(tx)
	for _, e := range elems {
		ret = ret.Child([]byte(e))
	}
	return
}
