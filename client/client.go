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

/*
Etcd client with simple interface for retries and timeouts.
It should be instanciated with the Connect function.
*/
type EtcdClient struct {
	Client         *clientv3.Client
	Retries        uint64
	RetryInterval  time.Duration
	RequestTimeout time.Duration
	Context        context.Context
	connOpts       EtcdClientOptions
}

/*
Returns a copy of the EtcdClient instance with a different Golang context.
Note that the underlying client connection to the etcd cluster is reused though.
Thus, a call to the Close method would impact both the original client and its copy.
*/
func (cli *EtcdClient) SetContext(ctx context.Context) *EtcdClient {
	return &EtcdClient{
		Client: cli.Client,
		Retries: cli.Retries,
		RequestTimeout: cli.RequestTimeout,
		Context: ctx,
		connOpts: cli.connOpts,
	}
}

/*
Returns a copy of the EtcdClient instance with a different underlying connection.
The endpoints of the new connection can be set in the argument.
Note however that the context is reused. To change it as well, a call to the SetContext can be made.
*/
func (cli *EtcdClient) SetEndpoints(endpoints []string) (*EtcdClient, error) {
	opts := cli.connOpts
	opts.EtcdEndpoints = endpoints
	return Connect(cli.Context, opts)
}

/*
Close the underlying connection to the etcd cluster that the client as.
*/
func (cli *EtcdClient) Close() error {
	return cli.Client.Close()
}

/*
Returns whether an error returned by etcd is probably transient and the operation should be retried
*/
func ErrorIsRetryable(err error) bool {
	etcdErr, ok := err.(rpctypes.EtcdError)
	if ok {
		if etcdErr.Code() != codes.Unavailable {
			return false
		}
	} else {
		stat, ok := status.FromError(err)
		if !ok {
			return false
		}

		if stat.Code() != codes.Unavailable && stat.Message() != raftv3.ErrProposalDropped.Error() {
			return false
		}
	}

	return true
}

func shouldRetry(err error, retries uint64) bool {
	if retries == 0 || (!ErrorIsRetryable(err)) {
		return false
	}

	return true
}
