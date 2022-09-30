package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func Connect(
	userCertPath string, 
	userKeyPath string,
	username string,
	password string, 
	caCertPath string, 
	etcdEndpoints string, 
	connectionTimeout uint64,
	requestTimeout uint64,
	retries uint64,
	) (*EtcdClient, error) {
	tlsConf := &tls.Config{}

	//User credentials
	if username == "" {
		certData, err := tls.LoadX509KeyPair(userCertPath, userKeyPath)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to load user credentials: %s", err.Error()))
		}
		(*tlsConf).Certificates = []tls.Certificate{certData}
	}

	(*tlsConf).InsecureSkipVerify = false
	
	//CA cert
	caCertContent, err := ioutil.ReadFile(caCertPath)
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

	if username == "" {
		cli, connErr = clientv3.New(clientv3.Config{
			Endpoints:   strings.Split(etcdEndpoints, ","),
			TLS:         tlsConf,
			DialTimeout: time.Duration(connectionTimeout) * time.Second,
		})
	} else {
		cli, connErr = clientv3.New(clientv3.Config{
			Username: username,
			Password: password,
			Endpoints:   strings.Split(etcdEndpoints, ","),
			TLS:         tlsConf,
			DialTimeout: time.Duration(connectionTimeout) * time.Second,
		})
	}
	
	if connErr != nil {
		return nil, errors.New(fmt.Sprintf("Failed to connect to etcd servers: %s", connErr.Error()))
	}
	
	return &EtcdClient{
		Client:         cli,
		Retries:        retries,
		RequestTimeout: requestTimeout,
	}, nil
}