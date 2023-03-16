package client

import "context"

func (cli *EtcdClient) AuthStatus() (bool, error) {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	resp, err := cli.Client.AuthStatus(ctx)
	if err != nil {
		return false, err
	}

	return resp.Enabled, nil
}

func (cli *EtcdClient) AuthSet(enable bool) error {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	var err error
	if enable {
		_, err = cli.Client.AuthEnable(ctx)
	} else {
		_, err = cli.Client.AuthDisable(ctx)
	}
	return err
}
