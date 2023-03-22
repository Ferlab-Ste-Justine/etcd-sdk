package client

import (
	"context"
	"time"

	"github.com/Ferlab-Ste-Justine/etcd-sdk/keymodels"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (cli *EtcdClient) getKeyRangeWithRetries(ctx context.Context, key string, rangeEnd string, retries uint64) (map[string]keymodels.KeyInfo, int64, error) {
	ictx, cancel := context.WithTimeout(ctx, cli.RequestTimeout)
	defer cancel()

	keys := make(map[string]keymodels.KeyInfo)

	res, err := cli.Client.Get(ictx, key, clientv3.WithRange(rangeEnd))
	if err != nil {
		if !shouldRetry(err, retries) {
			return keys, -1, err
		}

		time.Sleep(100 * time.Millisecond)
		return cli.getKeyRangeWithRetries(ctx, key, rangeEnd, retries-1)
	}

	for _, kv := range res.Kvs {
		key, value, createRevision, modRevision, version, lease := string(kv.Key), string(kv.Value), kv.CreateRevision, kv.ModRevision, kv.Version, kv.Lease
		keys[key] = keymodels.KeyInfo{
			Key:            key,
			Value:          value,
			Version:        version,
			CreateRevision: createRevision,
			ModRevision:    modRevision,
			Lease:          lease,
		}
	}

	return keys, res.Header.Revision, nil
}

func (cli *EtcdClient) GetKeyRange(ctx context.Context, key string, rangeEnd string) (map[string]keymodels.KeyInfo, int64, error) {
	return cli.getKeyRangeWithRetries(ctx, key, rangeEnd, cli.Retries)
}

func (cli *EtcdClient) deleteKeyRangeWithRetries(ctx context.Context, key string, rangeEnd string, retries uint64) error {
	ictx, cancel := context.WithTimeout(ctx, cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.Delete(ictx, key, clientv3.WithRange(rangeEnd))
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		return cli.deleteKeyRangeWithRetries(ctx, key, rangeEnd, retries-1)
	}

	return nil
}

func (cli *EtcdClient) DeleteKeyRange(ctx context.Context, key string, rangeEnd string) error {
	return cli.deleteKeyRangeWithRetries(ctx, key, rangeEnd, cli.Retries)
}
