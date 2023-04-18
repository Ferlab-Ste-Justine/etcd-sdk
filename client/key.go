package client

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

/*
Structure holding information returned on a specific key
*/
type KeyInfo struct {
	//Key
	Key            string
	//Value stored at the key
	Value          string
	//Etcd version of the key, which is incremented when a key changes and reset to 0 when it is deleted
	Version        int64
	//Revision of the etcd store when the key was created
	CreateRevision int64
	//Revision of the etcd store when the key was last modified
	ModRevision    int64
	//Id of the lease that created the key if the key was created with a lease
	Lease          int64
}

/*
Returns whether the KeyInfo structure stores a key that was found.
If the key is not found, an empty KeyInfo structure will be returned which will be detected by this method.
*/
func (info *KeyInfo) Found() bool {
	return info.CreateRevision > 0
}

func (cli *EtcdClient) putKeyWithRetries(key string, val string, retries uint64) error {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.Put(ctx, key, val)
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(cli.RetryInterval)
		return cli.putKeyWithRetries(key, val, retries-1)
	}
	return nil
}

/*
Upsert the given value in the key
*/
func (cli *EtcdClient) PutKey(key string, val string) error {
	return cli.putKeyWithRetries(key, val, cli.Retries)
}

func (cli *EtcdClient) getKeyWithRetries(key string, revision int64, retries uint64) (KeyInfo, error) {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	var err error
	var getRes *clientv3.GetResponse

	if revision <= 0 {
		getRes, err = cli.Client.Get(ctx, key)
	} else {
		getRes, err = cli.Client.Get(ctx, key, clientv3.WithRev(revision))
	}

	if err != nil {
		if !shouldRetry(err, retries) {
			return KeyInfo{}, err
		}

		time.Sleep(cli.RetryInterval)
		return cli.getKeyWithRetries(key, revision, retries-1)
	}

	if len(getRes.Kvs) == 0 {
		return KeyInfo{}, nil
	}

	return KeyInfo{
		Key:            key,
		Value:          string(getRes.Kvs[0].Value),
		Version:        getRes.Kvs[0].Version,
		CreateRevision: getRes.Kvs[0].CreateRevision,
		ModRevision:    getRes.Kvs[0].ModRevision,
		Lease:          getRes.Kvs[0].Lease,
	}, nil
}

/*
Options that get passed to GetKey method.
*/
type GetKeyOptions struct {
	//Specifies that the value of the key at a given store revision is wanted.
	//Can be left at the default 0 value if the latest version of the key is desired.
	Revision int64
}

/*
Get information on the given key including the value.
*/
func (cli *EtcdClient) GetKey(key string, opts GetKeyOptions) (KeyInfo, error) {
	return cli.getKeyWithRetries(key, opts.Revision, cli.Retries)
}

func (cli *EtcdClient) deleteKeyWithRetries(key string, retries uint64) error {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.Delete(ctx, key)
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(cli.RetryInterval)
		return cli.deleteKeyWithRetries(key, retries-1)
	}

	return nil
}

/*
Delete a key.
*/
func (cli *EtcdClient) DeleteKey(key string) error {
	return cli.deleteKeyWithRetries(key, cli.Retries)
}
