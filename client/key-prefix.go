package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type KeyWatchInfo struct {
	Value          string
	Version        int64
	CreateRevision int64
	ModRevision    int64
	Lease          int64
}

type WatchInfo struct {
	Upserts   map[string]KeyWatchInfo
	Deletions []string
}

type WatchNotification struct {
	Changes WatchInfo
	Error   error
}

type WatchOptions struct {
	Revision int64
	TrimPrefix bool
}

func (cli *EtcdClient) WatchPrefix(prefix string, opts WatchOptions) <-chan WatchNotification {
	outChan := make(chan WatchNotification)

	go func() {
		ctx, cancel := context.WithCancel(cli.Context)
		defer cancel()
		defer close(outChan)

		watchOpts := []clientv3.OpOption{clientv3.WithPrefix()}
		if opts.Revision > 0 {
			watchOpts = append(watchOpts, clientv3.WithRev(opts.Revision))
		}

		wc := cli.Client.Watch(ctx, prefix, watchOpts...)
		if wc == nil {
			outChan <- WatchNotification{Error: errors.New("Failed to watch prefix changes: Watcher could not be established")}
			return
		}

		for res := range wc {
			err := res.Err()
			if err != nil {
				outChan <- WatchNotification{Error: errors.New(fmt.Sprintf("Failed to watch prefix changes: %s", err.Error()))}
				return
			}

			output := WatchNotification{
				Error: nil,
				Changes: WatchInfo{
					Upserts:   make(map[string]KeyWatchInfo),
					Deletions: []string{},
				},
			}

			for _, ev := range res.Events {
				key := string(ev.Kv.Key)
				if opts.TrimPrefix {
					key = strings.TrimPrefix(key, prefix)
				}
				if ev.Type == mvccpb.DELETE {
					output.Changes.Deletions = append(
						output.Changes.Deletions, 
						key,
					)
				} else if ev.Type == mvccpb.PUT {
					output.Changes.Upserts[key] = KeyWatchInfo{
						Value: string(ev.Kv.Value),
						Version: ev.Kv.Version,
						CreateRevision: ev.Kv.CreateRevision,
						ModRevision: ev.Kv.ModRevision,
						Lease: ev.Kv.Lease,
					}
				}
			}

			outChan <- output
		}
	}()

	return outChan
}

/*
Convenience method that returns a KeyDiff structure containing all the operations that would need to be applied on the destination prefix to make it like the source prefix.
Note that for comparative purpose, a relative representation of both keyspaces without their respective prefixes is assumed.
*/
func (cli *EtcdClient) DiffBetweenPrefixes(srcPrefix string, dstPrefix string) (KeyDiff, error) {
	src, srcErr := cli.GetKeyRange(srcPrefix, clientv3.GetPrefixRangeEnd(srcPrefix))
	if srcErr != nil {
		return KeyDiff{}, srcErr
	}

	dst, dstErr := cli.GetKeyRange(dstPrefix, clientv3.GetPrefixRangeEnd(dstPrefix))
	if dstErr != nil {
		return KeyDiff{}, dstErr
	}

	return GetKeyDiff(src.Keys.ToValueMap(srcPrefix), dst.Keys.ToValueMap(dstPrefix)), nil
}

func (cli *EtcdClient) applyDiffToPrefixWithRetries(prefix string, diff KeyDiff, retries uint64) error {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	ops := []clientv3.Op{}

	for _, key := range diff.Deletions {
		ops = append(ops, clientv3.OpDelete(prefix + key))
	}

	for key, val := range diff.Inserts {
		ops = append(ops, clientv3.OpPut(prefix + key, val))
	}

	for key, val := range diff.Updates {
		ops = append(ops, clientv3.OpPut(prefix + key, val))
	}

	tx := cli.Client.Txn(ctx).Then(ops...)

	resp, txErr := tx.Commit()
	if txErr != nil {
		if !shouldRetry(txErr, retries) {
			return txErr
		}

		time.Sleep(cli.RetryInterval)
		return cli.applyDiffToPrefixWithRetries(prefix, diff, retries-1)
	}

	if !resp.Succeeded {
		return errors.New("Transaction failed")
	}

	return nil
}

/*
Applies the operation predicated by KeyDiff argument on all the keys prefixed with a given value.
Note that all the keys referenced in the KeyDiff structure are assumed to be relative keys without the prefix.
As such, the prefix will be prepended to all the keys in the Keydiff before applying the operations.
Also note that all the operations in the KeyDiff are applied atomically in a single transaction.
*/
func (cli *EtcdClient) ApplyDiffToPrefix(prefix string, diff KeyDiff) error {
	return cli.applyDiffToPrefixWithRetries(prefix, diff, cli.Retries)
}

/*
Delete all the keys that are prefixed by a given value
*/
func (cli *EtcdClient) DeletePrefix(prefix string) error {
	return cli.DeleteKeyRange(prefix, clientv3.GetPrefixRangeEnd(prefix))
}

/*
Get all the keys that are prefixed by a given value
*/
func (cli *EtcdClient) GetPrefix(prefix string) (KeyRangeInfo, error) {
	return cli.GetKeyRange(prefix, clientv3.GetPrefixRangeEnd(prefix))
}