package client

import (
	"context"
	"testing"
	"time"
)

func TestAuthEnableDisable(t *testing.T) {
	duration, _ := time.ParseDuration("5s")
	cli, err := Connect(context.Background(), EtcdClientOptions{
		ClientCertPath:    "../test/certs/root.pem",
		ClientKeyPath:     "../test/certs/root.key",
		CaCertPath:        "../test/certs/ca.pem",
		EtcdEndpoints:     []string{"127.0.0.1:3379", "127.0.0.2:3379", "127.0.0.3:3379"},
		ConnectionTimeout: duration,
		RequestTimeout:    duration,
		RetryInterval:     duration,
		Retries:           5,
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

	for i:=0; i < 200; i++ {
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

	err = cli.DeleteUser("root")
	if err != nil {
		t.Errorf("Auth test failed at the root user cleanup stage: %s", err.Error())
	}
}