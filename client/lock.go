package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Lock struct {
	Lease     clientv3.LeaseID
	Ttl       int64
	Timestamp time.Time
	Revision  int64
}

func (cli *EtcdClient) releaseLeaseWithRetries(lease clientv3.LeaseID, retries uint64) error {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.Revoke(ctx, lease)
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(cli.RetryInterval)
		return cli.releaseLeaseWithRetries(lease, retries-1)
	}

	return nil
}

func (cli *EtcdClient) acquireLockWithRetries(opts AcquireLockOptions, deadline time.Time, retries uint64) (*Lock, bool, error) {
	//If acquisition deadline has expired, fail
	now := time.Now()
	if now.After(deadline) {
		return nil, true, errors.New(fmt.Sprintf("Could not acquire lock on key %s before deadline", opts.Key))
	}

	//Exploratory get without getting a lease to see if a lock already exists
	//Seems more efficient not to create a lease unless likelyhood is high we can get a lock
	info, err := cli.GetKey(opts.Key, GetKeyOptions{})
	if err != nil {
		if !shouldRetry(err, retries) {
			return nil, false, err
		}

		time.Sleep(cli.RetryInterval)
		return cli.acquireLockWithRetries(opts, deadline, retries-1)
	}

	if info.Found() {
		time.Sleep(opts.RetryInterval)
		return cli.acquireLockWithRetries(opts, deadline, retries)
	}

	//Changes are good we can get a lock, so create a lease
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	leaseResp, leaseErr := cli.Client.Grant(ctx, opts.Ttl)
	if leaseErr != nil {
		if !shouldRetry(leaseErr, retries) {
			return nil, false, leaseErr
		}

		time.Sleep(cli.RetryInterval)
		return cli.acquireLockWithRetries(opts, deadline, retries-1)
	}

	//Create a lock with a transaction as safeguard, in case another acquirer narrowly beat us to the punch
	lock := Lock{
		Lease:     leaseResp.ID,
		Ttl:       opts.Ttl,
		Timestamp: now,
		Revision: leaseResp.ResponseHeader.Revision,
	}
	output, _ := json.Marshal(lock)

	txIfs := []clientv3.Cmp{clientv3.Compare(clientv3.Version(opts.Key), "=", 0)}
	if len(opts.ExtraConditions) > 0 {
		txIfs = append(txIfs,opts.ExtraConditions...)
	}

	txCtx, txCancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer txCancel()
	tx := cli.Client.Txn(txCtx).If(
		txIfs...
	).Then(
		clientv3.OpPut(opts.Key, string(output), clientv3.WithLease(leaseResp.ID)),
	)
	txResp, txErr := tx.Commit()

	//Transaction error
	if txErr != nil {
		releaseErr := cli.releaseLeaseWithRetries(leaseResp.ID, cli.Retries)
		if (!shouldRetry(txErr, retries)) || releaseErr != nil {
			return nil, false, txErr
		}

		time.Sleep(cli.RetryInterval)
		return cli.acquireLockWithRetries(opts, deadline, retries-1)
	}

	//Someone beat us to the punch acquiring the lock
	if !txResp.Succeeded {
		releaseErr := cli.releaseLeaseWithRetries(leaseResp.ID, cli.Retries)
		if releaseErr != nil {
			return nil, false, releaseErr
		}

		time.Sleep(opts.RetryInterval)
		return cli.acquireLockWithRetries(opts, deadline, retries)
	}

	return &lock, false, nil
}

type AcquireLockOptions struct {
	Key             string
	Ttl             int64
	Timeout         time.Duration
	RetryInterval   time.Duration
	ExtraConditions []clientv3.Cmp
}

func (cli *EtcdClient) AcquireLock(opts AcquireLockOptions) (*Lock, bool, error) {
	if opts.Ttl == 0 {
		opts.Ttl = 600
	}
	if int64(opts.Timeout) == 0 {
		opts.Timeout = 30 * time.Second
	}
	if int64(opts.RetryInterval) == 0 {
		opts.RetryInterval = 500 * time.Millisecond
	}

	now := time.Now()
	return cli.acquireLockWithRetries(opts, now.Add(opts.Timeout), cli.Retries)
}

func (cli *EtcdClient) ReadLock(key string) (*Lock, error) {
	info, err := cli.GetKey(key, GetKeyOptions{})
	if err != nil {
		return nil, err
	}
	if !info.Found() {
		return nil, errors.New(fmt.Sprintf("Could not acquire lock at key %s as it didn't exist", key))
	}

	lock := Lock{}
	unmarshalErr := json.Unmarshal([]byte(info.Value), &lock)
	if unmarshalErr != nil {
		return nil, unmarshalErr
	}

	return &lock, nil
}

func (cli *EtcdClient) ReleaseLock(key string) error {
	lock, lockErr := cli.ReadLock(key)
	if lockErr != nil {
		return lockErr
	}

	releaseErr := cli.releaseLeaseWithRetries(lock.Lease, cli.Retries)
	return releaseErr
}
