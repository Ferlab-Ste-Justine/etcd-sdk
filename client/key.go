package client

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type KeyInfo struct {
	Key            string
	Value          string
	Version        int64
	CreateRevision int64
	ModRevision    int64
	Lease          int64
}

func (cli *EtcdClient) putKeyWithRetries(key string, val string, retries uint64) error {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.Put(ctx, key, val)
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		return cli.putKeyWithRetries(key, val, retries-1)
	}
	return nil
}

func (cli *EtcdClient) PutKey(key string, val string) error {
	return cli.putKeyWithRetries(key, val, cli.Retries)
}

func (cli *EtcdClient) getKeyWithRetries(key string, revision int64, retries uint64) (KeyInfo, bool, error) {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	var err error
	var getRes *clientv3.GetResponse

	if revision == -1 {
		getRes, err = cli.Client.Get(ctx, key)
	} else {
		getRes, err = cli.Client.Get(ctx, key, clientv3.WithRev(revision))
	}

	if err != nil {
		if !shouldRetry(err, retries) {
			return KeyInfo{}, false, err
		}

		time.Sleep(100 * time.Millisecond)
		return cli.getKeyWithRetries(key, revision, retries-1)
	}

	if len(getRes.Kvs) == 0 {
		return KeyInfo{}, false, nil
	}

	return KeyInfo{
		Key:            key,
		Value:          string(getRes.Kvs[0].Value),
		Version:        getRes.Kvs[0].Version,
		CreateRevision: getRes.Kvs[0].CreateRevision,
		ModRevision:    getRes.Kvs[0].ModRevision,
		Lease:          getRes.Kvs[0].Lease,
	}, true, nil
}

func (cli *EtcdClient) GetKey(key string) (KeyInfo, bool, error) {
	return cli.getKeyWithRetries(key, -1, cli.Retries)
}

func (cli *EtcdClient) GetKeyAtRevision(key string, revision int64) (KeyInfo, bool, error) {
	return cli.getKeyWithRetries(key, revision, cli.Retries)
}

func (cli *EtcdClient) deleteKeyWithRetries(key string, retries uint64) error {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.Delete(ctx, key)
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		return cli.deleteKeyWithRetries(key, retries-1)
	}

	return nil
}

func (cli *EtcdClient) DeleteKey(key string) error {
	return cli.deleteKeyWithRetries(key, cli.Retries)
}
