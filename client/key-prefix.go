package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Ferlab-Ste-Justine/etcd-sdk/keymodels"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type PrefixChangesResult struct {
	Changes keymodels.WatchInfo
	Error   error
}

func (cli *EtcdClient) WatchPrefixChanges(ctx context.Context, prefix string, revision int64, trimPrefix bool) <-chan PrefixChangesResult {
	outChan := make(chan PrefixChangesResult)

	go func() {
		ictx, cancel := context.WithCancel(ctx)
		defer cancel()
		defer close(outChan)

		wc := cli.Client.Watch(ictx, prefix, clientv3.WithPrefix(), clientv3.WithRev(revision))
		if wc == nil {
			outChan <- PrefixChangesResult{Error: errors.New("Failed to watch prefix changes: Watcher could not be established")}
			return
		}

		for res := range wc {
			err := res.Err()
			if err != nil {
				outChan <- PrefixChangesResult{Error: errors.New(fmt.Sprintf("Failed to watch prefix changes: %s", err.Error()))}
				return
			}

			output := PrefixChangesResult{
				Error: nil,
				Changes: keymodels.WatchInfo{
					Upserts:   make(map[string]keymodels.KeyWatchInfo),
					Deletions: []string{},
				},
			}

			for _, ev := range res.Events {
				key := string(ev.Kv.Key)
				if trimPrefix {
					key = strings.TrimPrefix(key, prefix)
				}
				if ev.Type == mvccpb.DELETE {
					output.Changes.Deletions = append(
						output.Changes.Deletions, 
						key,
					)
				} else if ev.Type == mvccpb.PUT {
					output.Changes.Upserts[key] = keymodels.KeyWatchInfo{
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

func (cli *EtcdClient) DiffBetweenPrefixes(ctx context.Context, srcPrefix string, dstPrefix string) (keymodels.KeysDiff, error) {
	srcKeys, _, srcErr := cli.GetKeyRange(ctx, srcPrefix, clientv3.GetPrefixRangeEnd(srcPrefix))
	if srcErr != nil {
		return keymodels.KeysDiff{}, srcErr
	}

	dstKeys, _, dstErr := cli.GetKeyRange(ctx, dstPrefix, clientv3.GetPrefixRangeEnd(dstPrefix))
	if dstErr != nil {
		return keymodels.KeysDiff{}, dstErr
	}

	return keymodels.GetKeysDiff(srcKeys, srcPrefix, dstKeys, dstPrefix), nil
}

func (cli *EtcdClient) applyDiffToPrefixWithRetries(ctx context.Context, prefix string, diff keymodels.KeysDiff, retries uint64) error {
	ictx, cancel := context.WithTimeout(ctx, cli.RequestTimeout)
	defer cancel()

	ops := []clientv3.Op{}

	for _, key := range diff.Deletions {
		ops = append(ops, clientv3.OpDelete(prefix+key))
	}

	for key, val := range diff.Upserts {
		ops = append(ops, clientv3.OpPut(prefix+key, val))
	}

	tx := cli.Client.Txn(ictx).Then(ops...)

	resp, txErr := tx.Commit()
	if txErr != nil {
		if !shouldRetry(txErr, retries) {
			return txErr
		}

		time.Sleep(100 * time.Millisecond)
		return cli.applyDiffToPrefixWithRetries(ctx, prefix, diff, retries-1)
	}

	if !resp.Succeeded {
		return errors.New("Transaction failed")
	}

	return nil
}

func (cli *EtcdClient) ApplyDiffToPrefix(ctx context.Context, prefix string, diff keymodels.KeysDiff) error {
	return cli.applyDiffToPrefixWithRetries(ctx, prefix, diff, cli.Retries)
}

func (cli *EtcdClient) DiffPrefixWithMap(ctx context.Context, prefix string, inputKeys map[string]keymodels.KeyInfo, inputKeysPrefix string, inputIsSource bool) (keymodels.KeysDiff, error) {
	prefixKeys, _, err := cli.GetKeyRange(ctx, prefix, clientv3.GetPrefixRangeEnd(prefix))
	if err != nil {
		return keymodels.KeysDiff{}, err
	}

	if inputIsSource {
		return keymodels.GetKeysDiff(inputKeys, inputKeysPrefix, prefixKeys, prefix), nil
	}

	return keymodels.GetKeysDiff(prefixKeys, prefix, inputKeys, inputKeysPrefix), nil
}

func (cli *EtcdClient) DeletePrefix(ctx context.Context, prefix string) error {
	return cli.DeleteKeyRange(ctx, prefix, clientv3.GetPrefixRangeEnd(prefix))
}

func (cli *EtcdClient) GetPrefix(ctx context.Context, prefix string) (map[string]keymodels.KeyInfo, int64, error) {
	return cli.GetKeyRange(ctx, prefix, clientv3.GetPrefixRangeEnd(prefix))
}
