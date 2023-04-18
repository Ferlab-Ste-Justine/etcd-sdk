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

type GetKeyOptions struct {
	Revision int64
}

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

func (cli *EtcdClient) DeleteKey(key string) error {
	return cli.deleteKeyWithRetries(key, cli.Retries)
}
