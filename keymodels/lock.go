package keymodels

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

type Lock struct {
	Lease     clientv3.LeaseID
	Ttl       int64
	Timestamp time.Time
}
