package client

import (
	"context"
	"testing"
	"time"
	"sync"
)

func TestAuthEnableDisable(t *testing.T) {
	retryInterval, _ := time.ParseDuration("1s")
	timeouts, _ := time.ParseDuration("30s")
	cli, err := Connect(context.Background(), EtcdClientOptions{
		ClientCertPath:    "../test/certs/root.pem",
		ClientKeyPath:     "../test/certs/root.key",
		CaCertPath:        "../test/certs/ca.pem",
		EtcdEndpoints:     []string{"127.0.0.1:3379", "127.0.0.2:3379", "127.0.0.3:3379"},
		ConnectionTimeout: timeouts,
		RequestTimeout:    timeouts,
		RetryInterval:     retryInterval,
		Retries:           30,
	})

	if err != nil {
		t.Errorf("Auth test failed at the connection stage: %s", err.Error())
	}

	user := EtcdUser{
		Username: "root",
		Password: "",
		Roles: []string{"root"},
	}

	err = cli.UpsertUser(user)
	if err != nil {
		t.Errorf("Auth test failed at the root user creation stage: %s", err.Error())
	}

	status, statusErr := cli.GetAuthStatus()
	if statusErr != nil {
		t.Errorf("Auth test failed when fetching status: %s", statusErr.Error())
	}

	if status {
		t.Errorf("Auth test failed. Expected auth to be disabled as the initial state and it wasn't.")
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	go keepChangingLeaderInBackground(t, cli, done, &wg)

	for i:=0; i < 300; i++ {
		statusErr = cli.SetAuthStatus(true)
		if statusErr != nil {
			t.Errorf("Auth test failed when setting status: %s", statusErr.Error())
		}

		status, statusErr := cli.GetAuthStatus()
		if statusErr != nil {
			t.Errorf("Auth test failed when fetching status: %s", statusErr.Error())
		}

		if !status {
			t.Errorf("Auth test failed. Expected auth to be enabled after enabling it and it wasn't.")
		}

		statusErr = cli.SetAuthStatus(false)
		if statusErr != nil {
			t.Errorf("Auth test failed when setting status: %s", statusErr.Error())
		}

		status, statusErr = cli.GetAuthStatus()
		if statusErr != nil {
			t.Errorf("Auth test failed when fetching status: %s", statusErr.Error())
		}

		if status {
			t.Errorf("Auth test failed. Expected auth to be disabled after disabling it and it wasn't.")
		}
	}
	close(done)
	wg.Wait()

	err = cli.DeleteUser("root")
	if err != nil {
		t.Errorf("Auth test failed at the root user cleanup stage: %s", err.Error())
	}
}