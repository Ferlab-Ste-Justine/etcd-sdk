package client

import (
	"context"
	"time"

	"github.com/Ferlab-Ste-Justine/etcd-sdk/keymodels"
	"google.golang.org/grpc/codes"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (cli *EtcdClient) getKeyRangeWithRetries(key string, rangeEnd string, retries uint64) (map[string]keymodels.KeyInfo, int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cli.RequestTimeout)*time.Second)
	defer cancel()

	keys := make(map[string]keymodels.KeyInfo)

	res, err := cli.Client.Get(ctx, key, clientv3.WithRange(rangeEnd))
	if err != nil {
		etcdErr, ok := err.(rpctypes.EtcdError)
		if !ok {
			return keys, -1, err
		}
		
		if etcdErr.Code() != codes.Unavailable || retries == 0 {
			return keys, -1, err
		}

		time.Sleep(100 * time.Millisecond)
		return cli.getKeyRangeWithRetries(key, rangeEnd, retries - 1)
	}

	for _, kv := range res.Kvs {
		key, value, createRevision, modRevision, version, lease := string(kv.Key), string(kv.Value), kv.CreateRevision, kv.ModRevision, kv.Version, kv.Lease
		keys[key] = keymodels.KeyInfo{
			Key: key,
			Value: value,
			Version: version,
			CreateRevision: createRevision,
			ModRevision: modRevision,
			Lease: lease,
		}
	}

	return keys, res.Header.Revision, nil
}

func (cli *EtcdClient) GetKeyRange(key string, rangeEnd string) (map[string]keymodels.KeyInfo, int64, error) {
	return cli.getKeyRangeWithRetries(key, rangeEnd, cli.Retries)
}

func (cli *EtcdClient) deleteKeyRangeWithRetries(key string, rangeEnd string , retries int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cli.Timeout)*time.Second)
	defer cancel()

	_, err := cli.Client.Delete(ctx, key, clientv3.WithRange(rangeEnd))
	if err != nil {
		etcdErr, ok := err.(rpctypes.EtcdError)
		if !ok {
			return err
		}
		
		if etcdErr.Code() != codes.Unavailable || retries <= 0 {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		return cli.deleteKeyRangeWithRetries(key, rangeEnd, retries - 1)
	}

	return nil
}

func (cli *EtcdClient) DeleteKeyRange(key string, rangeEnd string) error {
	return cli.deleteKeyRangeWithRetries(key, rangeEnd, cli.Retries)
}