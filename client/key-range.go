package client

import (
	"context"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type KeyInfoMap map[string]KeyInfo

func (info *KeyInfoMap) ToValueMap(prefixTrim string) map[string]string {
	res := make(map[string]string)

	for key, val := range *info {
		key := strings.TrimPrefix(key, prefixTrim)
		res[key] = val.Value
	}

	return res
}

type KeyRangeInfo struct {
	Keys KeyInfoMap
	Revision int64
}

func (cli *EtcdClient) getKeyRangeWithRetries(key string, rangeEnd string, retries uint64) (KeyRangeInfo, error) {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	keys := KeyInfoMap(make(map[string]KeyInfo))

	res, err := cli.Client.Get(ctx, key, clientv3.WithRange(rangeEnd))
	if err != nil {
		if !shouldRetry(err, retries) {
			return KeyRangeInfo{
				Keys: keys, 
				Revision: -1,
			}, err
		}

		time.Sleep(cli.RetryInterval)
		return cli.getKeyRangeWithRetries(key, rangeEnd, retries-1)
	}

	for _, kv := range res.Kvs {
		key, value, createRevision, modRevision, version, lease := string(kv.Key), string(kv.Value), kv.CreateRevision, kv.ModRevision, kv.Version, kv.Lease
		keys[key] = KeyInfo{
			Key:            key,
			Value:          value,
			Version:        version,
			CreateRevision: createRevision,
			ModRevision:    modRevision,
			Lease:          lease,
		}
	}

	return KeyRangeInfo{
		Keys: keys, 
		Revision: res.Header.Revision,
	}, nil
}

func (cli *EtcdClient) GetKeyRange(key string, rangeEnd string) (KeyRangeInfo, error) {
	return cli.getKeyRangeWithRetries(key, rangeEnd, cli.Retries)
}

func (cli *EtcdClient) deleteKeyRangeWithRetries(key string, rangeEnd string, retries uint64) error {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.Delete(ctx, key, clientv3.WithRange(rangeEnd))
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(cli.RetryInterval)
		return cli.deleteKeyRangeWithRetries(key, rangeEnd, retries-1)
	}

	return nil
}

func (cli *EtcdClient) DeleteKeyRange(key string, rangeEnd string) error {
	return cli.deleteKeyRangeWithRetries(key, rangeEnd, cli.Retries)
}
