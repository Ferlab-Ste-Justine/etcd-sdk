package client

import (
	"context"
	"time"

	"github.com/Ferlab-Ste-Justine/etcd-sdk/keymodels"
	"google.golang.org/grpc/codes"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (cli *EtcdClient) putKeyWithRetries(key string, val string, retries int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cli.Timeout)*time.Second)
	defer cancel()

	_, err := cli.Client.Put(ctx, key, val)
	if err != nil {
		etcdErr, ok := err.(rpctypes.EtcdError)
		if !ok {
			return err
		}
		
		if etcdErr.Code() != codes.Unavailable || retries <= 0 {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		return cli.putKeyWithRetries(key, val, retries - 1)
	}
	return nil
}

func (cli *EtcdClient) PutKey(key string, val string) error {
	return cli.putKeyWithRetries(key, val, cli.Retries)
}

func (cli *EtcdClient) getKeyWithRetries(key string, revision int64, retries int) (KeyInfo, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cli.Timeout)*time.Second)
	defer cancel()

	var err error
	var getRes *clientv3.GetResponse

	if revision == -1 {
		getRes, err = cli.Client.Get(ctx, key)
	} else {
		getRes, err = cli.Client.Get(ctx, key, clientv3.WithRev(revision))
	}

	if err != nil {
		etcdErr, ok := err.(rpctypes.EtcdError)
		if !ok {
			return KeyInfo{}, false, err
		}
		
		if etcdErr.Code() != codes.Unavailable || retries <= 0 {
			return KeyInfo{}, false, err
		}

		time.Sleep(100 * time.Millisecond)
		return cli.getKeyWithRetries(key, revision, retries - 1)
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

func (cli *EtcdClient) deleteKeyWithRetries(key string, retries int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cli.Timeout)*time.Second)
	defer cancel()

	_, err := cli.Client.Delete(ctx, key)
	if err != nil {
		etcdErr, ok := err.(rpctypes.EtcdError)
		if !ok {
			return err
		}
		
		if etcdErr.Code() != codes.Unavailable || retries <= 0 {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		return cli.deleteKeyWithRetries(key, retries - 1)
	}

	return nil
}

func (cli *EtcdClient) DeleteKey(key string) error {
	return cli.deleteKeyWithRetries(key, cli.Retries)
}