package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"google.golang.org/grpc/connectivity"
	clientv3 "go.etcd.io/etcd/client/v3"
)

/*
Etcd connection options for the client.
*/
type EtcdClientOptions struct {
	//If tls is enabled and certificate authentication is used, path to the client certificate file
	ClientCertPath    string
	//If tls is enabled and certificate authentication is used, path to the client private key file
	ClientKeyPath     string
	//If tls is enabled, path to the CA certificate used to sign etcd's server certificates. 
	CaCertPath        string
	//If password authentication is used, name of the user.
	Username          string
	//If password authentication is used, password of the user.
	Password          string
	//Endpoints of the etcd cluster. Each entry should be of the format 'address:port'
	EtcdEndpoints     []string
	//Timeout for the initial connection attempt to the etcd cluster
	ConnectionTimeout time.Duration
	//Timeout for individual requests to the etcd cluster
	RequestTimeout    time.Duration
	//Interval of time to wait before retrying when requests to the etcd cluster fail
	RetryInterval     time.Duration
	//Number of retries to attempt before returning an error when requests to the etcd cluster fail
	Retries           uint64
	//If set to true, connection to the etcd cluster will be attempted in plaintext without encryption
	SkipTLS           bool
}

func getTlsConfigs(opts EtcdClientOptions) (*tls.Config, error) {
	tlsConf := &tls.Config{}

	//User credentials
	if opts.Username == "" {
		certData, err := tls.LoadX509KeyPair(opts.ClientCertPath, opts.ClientKeyPath)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to load user credentials: %s", err.Error()))
		}
		(*tlsConf).Certificates = []tls.Certificate{certData}
	}

	(*tlsConf).InsecureSkipVerify = false

	//CA cert
	caCertContent, err := ioutil.ReadFile(opts.CaCertPath)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to read root certificate file: %s", err.Error()))
	}
	roots := x509.NewCertPool()
	ok := roots.AppendCertsFromPEM(caCertContent)
	if !ok {
		return nil, errors.New("Failed to parse root certificate authority")
	}
	(*tlsConf).RootCAs = roots

	return tlsConf, nil
}

/*
Entrypoint function to try connect to the etcd cluster and return a client.
*/
func Connect(ctx context.Context, opts EtcdClientOptions) (*EtcdClient, error) {
	var tlsConf *tls.Config
	var tlsConfErr error

	if !opts.SkipTLS {
		tlsConf, tlsConfErr = getTlsConfigs(opts)
		if tlsConfErr != nil {
			return nil, tlsConfErr
		}
	}

	var cli *clientv3.Client
	var connErr error

	if opts.Username == "" {
		cli, connErr = clientv3.New(clientv3.Config{
			Context:     ctx,
			Endpoints:   opts.EtcdEndpoints,
			TLS:         tlsConf,
			DialTimeout: opts.ConnectionTimeout,
		})
	} else {
		cli, connErr = clientv3.New(clientv3.Config{
			Context:     ctx,
			Username:    opts.Username,
			Password:    opts.Password,
			Endpoints:   opts.EtcdEndpoints,
			TLS:         tlsConf,
			DialTimeout: opts.ConnectionTimeout,
		})
	}

	if connErr != nil {
		return nil, errors.New(fmt.Sprintf("Failed to connect to etcd servers: %s", connErr.Error()))
	}

	connDeadline := time.NewTimer(opts.ConnectionTimeout)
	defer connDeadline.Stop()
	state := cli.ActiveConnection().GetState()
	for state == connectivity.Connecting || state == connectivity.TransientFailure || state == connectivity.Idle {
		select {
		case <-connDeadline.C:
			cli.Close()
			return nil, errors.New("Failed to establish connection to etcd servers in time")
		case <-time.After(10 * time.Nanosecond):
		}
		state = cli.ActiveConnection().GetState()
	}

	return &EtcdClient{
		Client:         cli,
		Retries:        opts.Retries,
		RetryInterval:  opts.RetryInterval,
		RequestTimeout: opts.RequestTimeout,
		Context:        ctx,
		connOpts:       opts,
	}, nil
}