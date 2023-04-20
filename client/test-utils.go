package client

import (
	"context"
	"testing"
	"sync"
	"time"
)

func keepChangingLeaderInBackground(t *testing.T, cli *EtcdClient, done <-chan struct{}, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	d, _ := time.ParseDuration("1s")

	for true {
		select {
		case <-time.After(d):
			err := cli.ChangeLeader()
			if err != nil {
				t.Errorf("Error occured while changing leader in the background: %s", err.Error())
			}
		case <-done:
			return
		}
	}
}

func setupTestEnv(t *testing.T, timeouts time.Duration, retryInt time.Duration, retries uint64) *EtcdClient {
	cli, err := Connect(context.Background(), EtcdClientOptions{
		ClientCertPath:    "../test/certs/root.pem",
		ClientKeyPath:     "../test/certs/root.key",
		CaCertPath:        "../test/certs/ca.crt",
		EtcdEndpoints:     []string{"127.0.0.1:3379", "127.0.0.2:3379", "127.0.0.3:3379"},
		ConnectionTimeout: timeouts,
		RequestTimeout:    timeouts,
		RetryInterval:     retryInt,
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