package client

import (
	"context"
	"testing"
	"time"
)

func setupTestEnv(t *testing.T, duration time.Duration, retries uint64) *EtcdClient {
	cli, err := Connect(context.Background(), EtcdClientOptions{
		ClientCertPath:    "../test/certs/root.pem",
		ClientKeyPath:     "../test/certs/root.key",
		CaCertPath:        "../test/certs/ca.pem",
		EtcdEndpoints:     []string{"127.0.0.1:3379", "127.0.0.2:3379", "127.0.0.3:3379"},
		ConnectionTimeout: duration,
		RequestTimeout:    duration,
		RetryInterval:     duration,
		Retries:           retries,
	})

	if err != nil {
		t.Errorf("Test setup failed at the connection stage: %s", err.Error())
	}

	user := EtcdUser{
		Username: "root",
		Password: "",
		Roles: []string{"root"},
	}

	err = cli.UpsertUser(user)
	if err != nil {
		t.Errorf("Test setup failed at the root user creation stage: %s", err.Error())
	}

	err = cli.SetAuthStatus(true)
	if err != nil {
		t.Errorf("Test setup failed at the auth enabling stage: %s", err.Error())
	}

	return cli
}

func teardownTestEnv(t *testing.T, cli *EtcdClient) {
	err := cli.SetAuthStatus(false)
	if err != nil {
		t.Errorf("Test teardown failed at the auth disabling stage: %s", err.Error())
	}

	err = cli.DeleteUser("root")
	if err != nil {
		t.Errorf("Test teardown failed at the root user cleanup stage: %s", err.Error())
	}
}