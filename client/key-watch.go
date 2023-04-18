package client

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

/*
Information on key that got upserted as reported by the watch function
*/
type WatchKeyInfo struct {
	Value          string
	Version        int64
	CreateRevision int64
	ModRevision    int64
	Lease          int64
}

/*
Reported changes to key(s) of interest by the watch function
*/
type WatchInfo struct {
	//Inserted or updated keys. The map keys are the keys that were upserted.
	Upserts   map[string]WatchKeyInfo
	//List of keys that were deleted
	Deletions []string
}

/*
Events returned by the watch function.
It can report either a change or an error.
*/
type WatchNotification struct {
	//Changes that are reported if it is not an error
	Changes WatchInfo
	//Error that is reported if it is an error
	Error   error
}

/*
Options for the watch method
*/
type WatchOptions struct {
	//If non-zero, will watch from the given etcd store revision. Useful for retroactively watching past changes.
	Revision   int64
	//If true, it will assume the key argument is a prefix and will watch for all changes affecting keys with that prefix
	IsPrefix   bool
	//If true, it will trim the prefix value from all the keys in the reported changes. Useful if you are interested only in relative keys.
	TrimPrefix bool
}

/*
Watch the keys of a given prefix for changes and returns a channel that notifies of any changes
*/
func (cli *EtcdClient) Watch(wKey string, opts WatchOptions) <-chan WatchNotification {
	outChan := make(chan WatchNotification)

	go func() {
		ctx, cancel := context.WithCancel(cli.Context)
		defer cancel()
		defer close(outChan)

		watchOpts := []clientv3.OpOption{}
		if opts.IsPrefix {
			watchOpts = append(watchOpts, clientv3.WithPrefix())
		}

		if opts.Revision > 0 {
			watchOpts = append(watchOpts, clientv3.WithRev(opts.Revision))
		}

		wc := cli.Client.Watch(ctx, wKey, watchOpts...)
		if wc == nil {
			outChan <- WatchNotification{Error: errors.New("Failed to watch changes: Watcher could not be established")}
			return
		}

		for res := range wc {
			err := res.Err()
			if err != nil {
				outChan <- WatchNotification{Error: errors.New(fmt.Sprintf("Failed to watch changes: %s", err.Error()))}
				return
			}

			output := WatchNotification{
				Error: nil,
				Changes: WatchInfo{
					Upserts:   make(map[string]WatchKeyInfo),
					Deletions: []string{},
				},
			}

			for _, ev := range res.Events {
				key := string(ev.Kv.Key)
				if opts.TrimPrefix {
					key = strings.TrimPrefix(key, wKey)
				}
				if ev.Type == mvccpb.DELETE {
					output.Changes.Deletions = append(
						output.Changes.Deletions, 
						key,
					)
				} else if ev.Type == mvccpb.PUT {
					output.Changes.Upserts[key] = WatchKeyInfo{
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