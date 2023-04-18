package client

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
	Revision   int64
	IsPrefix   bool
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
					Upserts:   make(map[string]KeyWatchInfo),
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