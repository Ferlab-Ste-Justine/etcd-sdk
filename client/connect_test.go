package client

import (
	"context"
	"testing"
	"time"
)

func TestConnect(t *testing.T) {
	tearDown, launchErr := launchTestEtcdCluster("../test")
	if launchErr != nil {
		t.Errorf("Error occured launching test etcd cluster: %s", launchErr.Error())
		return
	}

	defer func() {
		errs := tearDown()
		if len(errs) > 0 {
			t.Errorf("Errors occured tearing down etcd cluster: %s", errs[0].Error())
		}
	}()

	duration, _ := time.ParseDuration("5s")
	_, err := Connect(context.Background(), EtcdClientOptions{
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
		t.Errorf("Connection test failed. Connection with right parameters should have been successful: %s", err.Error())
	}

	cli, connErr := Connect(context.Background(), EtcdClientOptions{
		ClientCertPath:    "../test/certs/root.pem",
		ClientKeyPath:     "../test/certs/root.key",
		CaCertPath:        "../test/certs/ca.pem",
		EtcdEndpoints:     []string{"127.0.0.11:3369", "127.0.0.12:3369", "127.0.0.13:3369"},
		ConnectionTimeout: duration,
		RequestTimeout:    duration,
		RetryInterval:     duration,
		Retries:           5,
	})

	if connErr == nil {
		t.Errorf("Connection test failed. Connection with wrong parameters should not have been successful. Connection status is: %v", cli.Client.ActiveConnection().GetState())
	}
}