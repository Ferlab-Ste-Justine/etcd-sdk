package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdRolePermission struct {
	Permission string
	Key        string
	RangeEnd   string
}

type EtcdRole struct {
	Name        string
	Permissions []EtcdRolePermission
}

func permissionToEnum(permission string) clientv3.PermissionType {
	if permission == "readwrite" {
		return clientv3.PermissionType(clientv3.PermReadWrite)
	} else if permission == "read" {
		return clientv3.PermissionType(clientv3.PermRead)
	} else {
		return clientv3.PermissionType(clientv3.PermWrite)
	}
}

func permissionEnumToPerm(perm clientv3.PermissionType) string {
	if perm == clientv3.PermissionType(clientv3.PermReadWrite) {
		return "readwrite"
	} else if perm == clientv3.PermissionType(clientv3.PermRead) {
		return "read"
	} else {
		return "write"
	}
}

func (cli *EtcdClient) listRolesWithRetries(retries uint64) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cli.RequestTimeout)
	defer cancel()

	res, err := cli.Client.RoleList(ctx)
	if err != nil {
		if !shouldRetry(err, retries) {
			return []string{}, err
		}

		time.Sleep(100 * time.Millisecond)
		return cli.listRolesWithRetries(retries - 1)
	}

	return res.Roles, nil
}

func (cli *EtcdClient) ListRoles() ([]string, error) {
	return cli.listRolesWithRetries(cli.Retries)
}

func (cli *EtcdClient) insertEmptyRoleWithRetries(name string, retries uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.RoleAdd(ctx, name)
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		return cli.insertEmptyRoleWithRetries(name, retries-1)
	}

	return nil
}

func (cli *EtcdClient) InsertEmptyRole(name string) error {
	return cli.insertEmptyRoleWithRetries(name, cli.Retries)
}

func (cli *EtcdClient) grantRolePermissionWithRetries(name string, permission EtcdRolePermission, retries uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.RoleGrantPermission(ctx, name, permission.Key, permission.RangeEnd, permissionToEnum(permission.Permission))
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		return cli.grantRolePermissionWithRetries(name, permission, retries-1)
	}

	return nil
}

func (cli *EtcdClient) GrantRolePermission(name string, permission EtcdRolePermission) error {
	return cli.grantRolePermissionWithRetries(name, permission, cli.Retries)
}

func (cli *EtcdClient) revokeRolePermissionWithRetries(name string, key string, rangeEnd string, retries uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.RoleRevokePermission(ctx, name, key, rangeEnd)
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		return cli.revokeRolePermissionWithRetries(name, key, rangeEnd, retries-1)
	}

	return nil
}

func (cli *EtcdClient) RevokeRolePermission(name string, key string, rangeEnd string) error {
	return cli.revokeRolePermissionWithRetries(name, key, rangeEnd, cli.Retries)
}

func (cli *EtcdClient) getRolePermissionsWithRetries(name string, retries uint64) ([]EtcdRolePermission, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cli.RequestTimeout)
	defer cancel()

	res, err := cli.Client.RoleGet(ctx, name)
	if err != nil {
		etcdErr, ok := err.(rpctypes.EtcdError)
		if ok && etcdErr.Error() == rpctypes.ErrorDesc(rpctypes.ErrGRPCRoleNotFound) {
			return []EtcdRolePermission{}, false, nil
		}

		if !shouldRetry(err, retries) {
			return []EtcdRolePermission{}, false, err
		}

		time.Sleep(100 * time.Millisecond)
		return cli.getRolePermissionsWithRetries(name, retries-1)
	}

	result := make([]EtcdRolePermission, len(res.Perm))
	for idx, _ := range res.Perm {
		perm := EtcdRolePermission{
			Permission: permissionEnumToPerm(clientv3.PermissionType(res.Perm[idx].PermType)),
			Key:        string(res.Perm[idx].Key),
			RangeEnd:   string(res.Perm[idx].RangeEnd),
		}
		result[idx] = perm
	}

	return result, true, nil
}

func (cli *EtcdClient) GetRolePermissions(name string) ([]EtcdRolePermission, bool, error) {
	return cli.getRolePermissionsWithRetries(name, cli.Retries)
}

func (cli *EtcdClient) deleteRoleWithRetries(name string, retries uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.RoleDelete(ctx, name)
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		return cli.deleteRoleWithRetries(name, retries-1)
	}

	return nil
}

func (cli *EtcdClient) DeleteRole(name string) error {
	return cli.deleteRoleWithRetries(name, cli.Retries)
}

func (cli *EtcdClient) InsertRole(role EtcdRole) error {
	err := cli.InsertEmptyRole(role.Name)
	if err != nil {
		return errors.New(fmt.Sprintf("Error creating new role '%s': %s", role.Name, err.Error()))
	}

	for _, permission := range role.Permissions {
		err := cli.GrantRolePermission(role.Name, permission)
		if err != nil {
			return errors.New(fmt.Sprintf("Error adding role permission (key='%s', range_end='%s', permission='%s') for role '%s': %s", permission.Key, permission.RangeEnd, permission.Permission, role.Name, err.Error()))
		}
	}

	return nil
}

func (cli *EtcdClient) UpdateRole(role EtcdRole) error {
	resPermissions, _, err := cli.GetRolePermissions(role.Name)
	if err != nil {
		return errors.New(fmt.Sprintf("Error retrieving existing role '%s' for update: %s", role.Name, err.Error()))
	}

	for _, resPermission := range resPermissions {
		remove := true
		for _, permission := range role.Permissions {
			if resPermission.Permission == permission.Permission && resPermission.Key == permission.Key && resPermission.RangeEnd == permission.RangeEnd {
				remove = false
			}
		}
		if remove {
			err := cli.RevokeRolePermission(role.Name, resPermission.Key, resPermission.RangeEnd)
			if err != nil {
				return errors.New(fmt.Sprintf("Error removing role permission (key='%s', range_end='%s', permission='%s') for role '%s': %s", resPermission.Key, resPermission.RangeEnd, resPermission.Permission, role.Name, err.Error()))
			}
		}
	}

	for _, permission := range role.Permissions {
		add := true
		for _, resPermission := range resPermissions {
			if resPermission.Permission == permission.Permission && resPermission.Key == permission.Key && resPermission.RangeEnd == permission.RangeEnd {
				add = false
			}
		}
		if add {
			err := cli.GrantRolePermission(role.Name, permission)
			if err != nil {
				return errors.New(fmt.Sprintf("Error adding role permission (key='%s', range_end='%s', permission='%s') for role '%s': %s", permission.Key, permission.RangeEnd, permission.Permission, role.Name, err.Error()))
			}
		}
	}

	return nil
}

func (cli *EtcdClient) UpsertRole(role EtcdRole) error {
	roles, err := cli.ListRoles()
	if err != nil {
		return errors.New(fmt.Sprintf("Error retrieving existing roles list: %s", err.Error()))
	}

	if isStringInSlice(role.Name, roles) {
		return cli.UpdateRole(role)
	}

	return cli.InsertRole(role)
}
