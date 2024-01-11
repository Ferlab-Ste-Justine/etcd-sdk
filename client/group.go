package client

import (
	"errors"
	"fmt"
)

/*
Join a group as represented by groupPrefix. A member with id memberId and content memberContent will be added.
*/
func (cli *EtcdClient) JoinGroup(groupPrefix string, memberId string, memberContent string) (error) {
	_, err := cli.PutKey(fmt.Sprintf("%s%s", groupPrefix, memberId), memberContent)
	return err
}

/*
Leave a group as represented by groupPrefix. A member with id memberId will be removed.
*/
func (cli *EtcdClient) LeaveGroup(groupPrefix string, memberId string) (error) {
	return cli.DeleteKey(fmt.Sprintf("%s%s", groupPrefix, memberId))
}

/*
Get a list of group members of a group represented by groupPrefix
First return value are a map of members, with its keys being member ids and values being the passed member contents.
Second return value is the etcd revision at the time the result was obtained
*/
func (cli *EtcdClient) GetGroupMembers(groupPrefix string) (map[string]string, int64, error) {
	info, err := cli.GetPrefix(groupPrefix)
	if err != nil {
		return nil, -1, err
	}

	return info.Keys.ToValueMap(groupPrefix), info.Revision, nil
}

/*
Wait until a group as represented by groupPrefix has reached a threshold number of members
Last argument is a done channel that can be closed to halt the wait.
Return argument is a channel that will received an error if there is an issue or otherwise will be closed when the wait condition is fulfilled
*/
func (cli *EtcdClient) WaitGroupCountThreshold(groupPrefix string, threshold int64, doneCh <-chan struct{}) <-chan error {
	errCh := make(chan error)
	go func() {
		defer close(errCh)
		members, rev, err := cli.GetGroupMembers(groupPrefix)
		if err != nil {
			errCh <- err
			return
		}

		if int64(len(members)) >= threshold {
			return
		}

		wcCh := cli.Watch(groupPrefix, WatchOptions{IsPrefix: true, TrimPrefix: true, Revision: rev + 1})
		for true {
			select {
			case res, ok :=  <-wcCh:
				if !ok {
					errCh <- errors.New("Watch stopped before reaching threshold")
					return
				}

				if res.Error != nil {
					errCh <- res.Error
					return
				}

				res.Changes.ApplyOn(members)
				if int64(len(members)) >= threshold {
					return
				}
			case <-doneCh:
				return
			}
		}
	}()
	return errCh
}