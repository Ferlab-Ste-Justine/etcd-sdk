package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

type EtcdUser struct {
	Username string
	Password string
	Roles    []string
}

func (cli *EtcdClient) listUsersWithRetries(retries uint64) ([]string, error) {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	res, err := cli.Client.UserList(ctx)
	if err != nil {
		if !shouldRetry(err, retries) {
			return []string{}, err
		}

		time.Sleep(cli.RetryInterval)
		return cli.listUsersWithRetries(retries - 1)
	}

	return res.Users, nil
}

func (cli *EtcdClient) ListUsers() ([]string, error) {
	return cli.listUsersWithRetries(cli.Retries)
}

func (cli *EtcdClient) insertEmptyUserWithRetries(username string, password string, retries uint64) error {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.UserAdd(ctx, username, password)
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(cli.RetryInterval)
		return cli.insertEmptyUserWithRetries(username, password, retries-1)
	}

	return nil
}

func (cli *EtcdClient) InsertEmptyUser(username string, password string) error {
	return cli.insertEmptyUserWithRetries(username, password, cli.Retries)
}

func (cli *EtcdClient) getUserRolesWithRetries(username string, retries uint64) ([]string, bool, error) {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	res, err := cli.Client.UserGet(ctx, username)
	if err != nil {
		etcdErr, ok := err.(rpctypes.EtcdError)
		if ok && etcdErr.Error() == rpctypes.ErrorDesc(rpctypes.ErrGRPCUserNotFound) {
			return []string{}, false, nil
		}

		if !shouldRetry(err, retries) {
			return []string{}, false, err
		}

		time.Sleep(cli.RetryInterval)
		return cli.getUserRolesWithRetries(username, retries-1)
	}

	return res.Roles, true, nil
}

func (cli *EtcdClient) GetUserRoles(username string) ([]string, bool, error) {
	return cli.getUserRolesWithRetries(username, cli.Retries)
}

func (cli *EtcdClient) changeUserPasswordWithRetries(username string, password string, retries uint64) error {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.UserChangePassword(ctx, username, password)
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(cli.RetryInterval)
		return cli.changeUserPasswordWithRetries(username, password, retries-1)
	}

	return nil
}

func (cli *EtcdClient) ChangeUserPassword(username string, password string) error {
	return cli.changeUserPasswordWithRetries(username, password, cli.Retries)
}

func (cli *EtcdClient) grantUserRoleWithRetries(username string, role string, retries uint64) error {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.UserGrantRole(ctx, username, role)
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(cli.RetryInterval)
		return cli.grantUserRoleWithRetries(username, role, retries-1)
	}

	return nil
}

func (cli *EtcdClient) GrantUserRole(username string, role string) error {
	return cli.grantUserRoleWithRetries(username, role, cli.Retries)
}

func (cli *EtcdClient) revokeUserRoleWithRetries(username string, role string, retries uint64) error {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.UserRevokeRole(ctx, username, role)
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(cli.RetryInterval)
		return cli.revokeUserRoleWithRetries(username, role, retries-1)
	}

	return nil
}

func (cli *EtcdClient) RevokeUserRole(username string, role string) error {
	return cli.revokeUserRoleWithRetries(username, role, cli.Retries)
}

func (cli *EtcdClient) deleteUserWithRetries(username string, retries uint64) error {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.UserDelete(ctx, username)
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(cli.RetryInterval)
		return cli.deleteUserWithRetries(username, retries-1)
	}

	return nil
}

func (cli *EtcdClient) DeleteUser(username string) error {
	return cli.deleteUserWithRetries(username, cli.Retries)
}

func (cli *EtcdClient) InsertUser(user EtcdUser) error {
	err := cli.InsertEmptyUser(user.Username, user.Password)
	if err != nil {
		return errors.New(fmt.Sprintf("Error creating new user '%s': %s", user.Username, err.Error()))
	}

	for _, role := range user.Roles {
		err := cli.GrantUserRole(user.Username, role)
		if err != nil {
			return errors.New(fmt.Sprintf("Error adding role '%s' to user '%s': %s", role, user.Username, err.Error()))
		}
	}

	return nil
}

func (cli *EtcdClient) UpdateUser(user EtcdUser) error {
	resRoles, _, userRolesErr := cli.GetUserRoles(user.Username)
	if userRolesErr != nil {
		return errors.New(fmt.Sprintf("Error retrieving existing user '%s' for update: %s", user.Username, userRolesErr.Error()))
	}

	passErr := cli.ChangeUserPassword(user.Username, user.Password)
	if passErr != nil {
		return errors.New(fmt.Sprintf("Error updating password of user '%s': %s", user.Username, passErr.Error()))
	}

	for _, role := range user.Roles {
		add := true
		for _, resRole := range resRoles {
			if role == resRole {
				add = false
			}
		}

		if add {
			err := cli.GrantUserRole(user.Username, role)
			if err != nil {
				return errors.New(fmt.Sprintf("Error adding role '%s' to user '%s': %s", role, user.Username, err.Error()))
			}
		}
	}

	for _, resRole := range resRoles {
		remove := true
		for _, role := range user.Roles {
			if resRole == role {
				remove = false
			}
		}

		if remove {
			err := cli.RevokeUserRole(user.Username, resRole)
			if err != nil {
				return errors.New(fmt.Sprintf("Error removing role '%s' from user '%s': %s", resRole, user.Username, err.Error()))
			}
		}
	}

	return nil
}

func (cli *EtcdClient) UpsertUser(user EtcdUser) error {
	users, err := cli.ListUsers()
	if err != nil {
		return errors.New(fmt.Sprintf("Error retrieving existing users list: %s", err.Error()))
	}

	if isStringInSlice(user.Username, users) {
		return cli.UpdateUser(user)
	}

	return cli.InsertUser(user)
}
