package keymodels

import (
	"time"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Lock struct {
	Lease     clientv3.LeaseID
	Ttl       int64
	Timestamp time.Time
}