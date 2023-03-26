package client

import clientv3 "go.etcd.io/etcd/client/v3"

func KeyIsPresent(key string, presence bool) clientv3.Cmp {
	if presence {
		return clientv3.Compare(clientv3.Version(key), "=", 0)
	}

	return clientv3.Compare(clientv3.Version(key), ">", 0)
}

func KeyValueIsCmp(key string, cmp string, value string) clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(key), cmp, value)
}

func KeyVersionIsCmp(key string, cmp string, value int64) clientv3.Cmp {
	return clientv3.Compare(clientv3.Version(key), cmp, value)
}