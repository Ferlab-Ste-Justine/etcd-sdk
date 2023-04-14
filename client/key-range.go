package client

import (
	"context"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

/*
A map of KeyInfo values with each key in the map being the KeyInfo's key.
This is used as a separate type mostly for convenience methods
*/
type KeyInfoMap map[string]KeyInfo

/*
Flatten a KeyInfoMap structure to a simple map of key/value pairs.
The method accepts a prefix argument that will be trimmed from the beginning of the keys.
*/
func (info *KeyInfoMap) ToValueMap(prefixTrim string) map[string]string {
	res := make(map[string]string)

	for key, val := range *info {
		key := strings.TrimPrefix(key, prefixTrim)
		res[key] = val.Value
	}

	return res
}

/*
Result from a key range query.
It returns a KeyInfoMap structure containing the result.
It also contains a revision indication the revision of the etcd store at the moment the results were returned.
*/
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

/*
Get all the keys within a certain range of values.
If you want to get all the keys prefixed by a certain value, consider using the GetPrefix method instead.
*/
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

/*
Delete all the keys within a certain range of values.
If you want to delete all the keys prefixed by a certain value, consider using the DeletePrefix method instead.
*/
func (cli *EtcdClient) DeleteKeyRange(key string, rangeEnd string) error {
	return cli.deleteKeyRangeWithRetries(key, rangeEnd, cli.Retries)
}
