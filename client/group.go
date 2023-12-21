package client

import (
	"errors"
	"fmt"
)

type Group struct {
	KeyPrefix string
	Id string
}

func (cli *EtcdClient) JoinGroup(gr Group) (error) {
	_, err := cli.PutKey(fmt.Sprintf("%s%s", gr.KeyPrefix, gr.Id), fmt.Sprintf("%s", gr.Id))
	return err
}

func (cli *EtcdClient) LeaveGroup(gr Group) (error) {
	return cli.DeleteKey(fmt.Sprintf("%s%s", gr.KeyPrefix, gr.Id))
}

func (cli *EtcdClient) GetGroupMembers(gr Group) (map[string]string, int64, error) {
	info, err := cli.GetPrefix(gr.KeyPrefix)
	if err != nil {
		return nil, -1, err
	}

	return info.Keys.ToValueMap(gr.KeyPrefix), info.Revision, nil
}

func (cli *EtcdClient) WaitGroupCountThreshold(gr Group, threshold int64, doneCh <-chan struct{}) <-chan error {
	errCh := make(chan error)
	go func() {
		members, rev, err := cli.GetGroupMembers(gr)
		if err != nil {
			errCh <- err
			close(errCh)
			return
		}

		if int64(len(members)) >= threshold {
			close(errCh)
			return
		}

		wcCh := cli.Watch(gr.KeyPrefix, WatchOptions{IsPrefix: true, TrimPrefix: true, Revision: rev + 1})
		for true {
			select {
			case res, ok :=  <-wcCh:
				if !ok {
					errCh <- errors.New("Watch stopped before reaching threshold")
					close(errCh)
					return
				}

				if res.Error != nil {
					errCh <- res.Error
					close(errCh)
					return
				}

				res.Changes.ApplyOn(members)
				if int64(len(members)) >= threshold {
					close(errCh)
					return
				}
			case <-doneCh:
				close(errCh)
				return
			}
		}
	}()
	return errCh
}