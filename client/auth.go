package client

import (
	"context"
	"time"
)

func (cli *EtcdClient) getAuthStatusWithRetries(retries uint64) (bool, error) {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	resp, err := cli.Client.AuthStatus(ctx)
	if err != nil {
		if !shouldRetry(err, retries) {
			return false, err
		}

		time.Sleep(cli.RetryInterval)
		return cli.getAuthStatusWithRetries(retries - 1)
	}

	return resp.Enabled, nil
}

/*
Retrieves the authentication status (enabled or not) of the etcd cluster.
*/
func (cli *EtcdClient) GetAuthStatus() (bool, error) {
	return cli.getAuthStatusWithRetries(cli.Retries)
}

func (cli *EtcdClient) setAuthStatusWithRetries(enabled bool, retries uint64) error {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	var err error
	if enabled {
		_, err = cli.Client.AuthEnable(ctx)
	} else {
		_, err = cli.Client.AuthDisable(ctx)
	}

	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(cli.RetryInterval)
		return cli.setAuthStatusWithRetries(enabled, retries - 1)
	}

	return nil
}

/*
Sets the authentication status (enabled or not) of the etcd cluster.
*/
func (cli *EtcdClient) SetAuthStatus(enable bool) error {
	return cli.setAuthStatusWithRetries(enable, cli.Retries)
}
