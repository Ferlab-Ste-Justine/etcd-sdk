package client

import (
	"context"
	"errors"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/snapshot"
)

func (cli *EtcdClient) Snapshot(onLeader bool, path string, snapshotTimeout time.Duration) error {
	members, membersErr := cli.GetMembers(true)
	if membersErr != nil {
		return membersErr
	}

	var selectedMember EtcdMember
	memberFound := false
	for _, member := range members.Members {
		if onLeader && member.Status.IsLeader {
			selectedMember = member
			memberFound = true
			break
		} else if (!onLeader) && (!member.Status.IsLeader) {
			selectedMember = member
			memberFound = true
			break
		}
	}

	if !memberFound {
		return errors.New("No member with the requests characteristics was found to get snapshot")
	}

	ctx, cancel := context.WithTimeout(cli.Context, snapshotTimeout)
	defer cancel()

    tlsConfs, tlsConfsErr := getTlsConfigs(cli.connOpts)
    if tlsConfsErr != nil {
        return tlsConfsErr
    }

	return snapshot.Save(ctx, nil, clientv3.Config{
		Context:     ctx,
		Username:    cli.connOpts.Username,
		Password:    cli.connOpts.Password,
		Endpoints:   []string{selectedMember.ClientUrls[0]},
		TLS:         tlsConfs,
		DialTimeout: cli.connOpts.ConnectionTimeout,
	}, path)
}