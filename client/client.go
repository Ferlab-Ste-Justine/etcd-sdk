package client

type EtcdClient struct {
	Client *clientv3.Client
	Retries uint64
	RequestTimeout uint64
}