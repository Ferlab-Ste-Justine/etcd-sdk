package client

import (
	"context"
	"testing"
	"time"
)

func TestConnect(t *testing.T) {
	failures := 0
	duration, _ := time.ParseDuration("1s")
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

	_, err = Connect(context.Background(), EtcdClientOptions{
		ClientCertPath:    "../test/certs/root.pem",
		ClientKeyPath:     "../test/certs/root.key",
		CaCertPath:        "../test/certs/ca.pem",
		EtcdEndpoints:     []string{"127.0.0.11:3379", "127.0.0.12:3379", "127.0.0.13:3379"},
		ConnectionTimeout: duration,
		RequestTimeout:    duration,
		RetryInterval:     duration,
		Retries:           5,
	})

	if err != nil {
		failures += 1
	}

	if failures != 0 {
		t.Errorf("Connection test failed. Connection with wrong parameters should not have been successful")
	}
}