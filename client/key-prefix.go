package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/Ferlab-Ste-Justine/etcd-sdk/keymodels"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type PrefixChangesResult struct {
	Changes keymodels.KeysDiff
	Error   error
}

func (cli *EtcdClient) WatchPrefixChanges(prefix string, revision int64) (<-chan PrefixChangesResult) {
	outChan := make(chan PrefixChangesResult)

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		defer close(outChan)

		wc := cli.Client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(revision))
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
				Changes: keymodels.KeysDiff{
					Upserts: make(map[string]string),
					Deletions: []string{},
				},
			}

			for _, ev := range res.Events {
				if ev.Type == mvccpb.DELETE {
					output.Changes.Deletions = append(output.Changes.Deletions, strings.TrimPrefix(string(ev.Kv.Key), prefix))
				} else if ev.Type == mvccpb.PUT {
					output.Changes.Upserts[strings.TrimPrefix(string(ev.Kv.Key), prefix)] = string(ev.Kv.Value)
				}
			}

			outChan <- output
		}
	}()

	return outChan
}

func (cli *EtcdClient) DiffBetweenPrefixes(srcPrefix string, dstPrefix string) (KeysDiff, error) {
	srcKeys, srcErr := cli.GetKeyRange(srcPrefix, clientv3.GetPrefixRangeEnd(srcPrefix))
	if srcErr != nil {
		return KeysDiff{}, srcErr
	}

	dstKeys, dstErr := cli.GetKeyRange(dstPrefix, clientv3.GetPrefixRangeEnd(dstPrefix))
	if dstErr != nil {
		return KeysDiff{}, dstErr
	}

	return keymodels.GetKeysDiff(srcKeys, srcPrefix, dstKeys, dstPrefix), nil
}

func (cli *EtcdClient) applyDiffToPrefixWithRetries(prefix string, diff KeysDiff, retries int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(conn.Timeout)*time.Second)
	defer cancel()

	ops := []clientv3.Op{}

	for _, key := range diff.Deletions {
		ops = append(ops, clientv3.OpDelete(prefix + key))
	}

	for key, val := range diff.Upserts {
		ops = append(ops, clientv3.OpPut(prefix + key, val))
	}
	
	tx := cli.Client.Txn(ctx).Then(ops...)

	resp, txErr := tx.Commit()
	if txErr != nil {
		etcdErr, ok := txErr.(rpctypes.EtcdError)
		if !ok {
			return txErr
		}
		
		if etcdErr.Code() != codes.Unavailable || retries <= 0 {
			return txErr
		}

		time.Sleep(100 * time.Millisecond)
		return cli.applyDiffToPrefixWithRetries(prefix, diff, retries - 1)
	}

	if !resp.Succeeded {
		return errors.New("Transaction failed")
	}

	return nil
}

func (cli *EtcdClient) ApplyDiffToPrefix(prefix string, diff KeysDiff) error {
	return cli.applyDiffToPrefixWithRetries(prefix, diff, cli.Retries)
}

func (cli *EtcdClient) DiffPrefixWithMap(prefix string, inputKeys map[string]KeyInfo, inputKeysPrefix string, inputIsSource bool) (KeysDiff, error) {
	prefixKeys, err := cli.GetKeyRange(prefix, clientv3.GetPrefixRangeEnd(prefix))
	if err != nil {
		return KeysDiff{}, err
	}

	if inputIsSource {
		return cli.GetKeysDiff(inputKeys, inputKeysPrefix, prefixKeys, prefix), nil
	}

	return cli.GetKeysDiff(prefixKeys, prefix, inputKeys, inputKeysPrefix), nil
}

func (cli *EtcdClient) DeletePrefix(prefix string) error {
	return cli.DeleteKeyRange(prefix, clientv3.GetPrefixRangeEnd(prefix))
}