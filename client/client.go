package client

import (
	"context"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	raftv3 "go.etcd.io/raft/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type EtcdClient struct {
	Client         *clientv3.Client
	Retries        uint64
	RetryInterval  time.Duration
	RequestTimeout time.Duration
	Context        context.Context
	connOpts       EtcdClientOptions
}

func (cli *EtcdClient) SetContext(ctx context.Context) *EtcdClient {
	return &EtcdClient{
		Client: cli.Client,
		Retries: cli.Retries,
		RequestTimeout: cli.RequestTimeout,
		Context: ctx,
	}
}

func (cli *EtcdClient) Close() {
	cli.Client.Close()
}

func shouldRetry(err error, retries uint64) bool {
	etcdErr, ok := err.(rpctypes.EtcdError)
	if ok {
		if etcdErr.Code() != codes.Unavailable || retries == 0 {
			return false
		}
	} else {
		stat, ok := status.FromError(err)
		if !ok {
			return false
		}

		if stat.Message() != raftv3.ErrProposalDropped.Error() {
			return false
		}
	}

	return true
}
