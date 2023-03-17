package client

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdClientOptions struct {
	ClientCertPath    string
	ClientKeyPath     string
	CaCertPath        string
	Username          string
	Password          string
	EtcdEndpoints     []string
	ConnectionTimeout time.Duration
	RequestTimeout    time.Duration
	Retries           uint64
}

func Connect(opts EtcdClientOptions) (*EtcdClient, error) {
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

	//Connection
	var cli *clientv3.Client
	var connErr error

	if opts.Username == "" {
		cli, connErr = clientv3.New(clientv3.Config{
			Endpoints:   opts.EtcdEndpoints,
			TLS:         tlsConf,
			DialTimeout: opts.ConnectionTimeout,
		})
	} else {
		cli, connErr = clientv3.New(clientv3.Config{
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

	return &EtcdClient{
		Client:         cli,
		Retries:        opts.Retries,
		RequestTimeout: opts.RequestTimeout,
	}, nil
}
