package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type ChunkedKeySnapshot struct {
	Info     ChunkedKeyInfo
	Revision int64
}

type ChunkedKeyInfo struct {
	Size    int64
	Count   int64
	Version int64
}

type ChunkedKeyPayload struct {
	Key   string
	Value io.ReadCloser
	Size  int64
}

func (p *ChunkedKeyPayload) Close() error {
	return p.Value.Close()
}

func (p *ChunkedKeyPayload) Read(r []byte) (n int, err error) {
	return p.Value.Read(r)
}

func (cli *EtcdClient) getChunkedKeyInfo(key string) (*ChunkedKeyInfo, int64, error) {
	info, exists, err := cli.GetKey(fmt.Sprintf("%s/info", key))
	if err != nil || (!exists) {
		return nil, 0, err
	}

	cKeyInfo := ChunkedKeyInfo{}
	unmarshalErr := json.Unmarshal([]byte(info.Value), &cKeyInfo)
	if unmarshalErr != nil {
		return nil, info.ModRevision, unmarshalErr
	}

	return &cKeyInfo, info.ModRevision, nil
}

func (cli *EtcdClient) persistVersionChange(key string, info ChunkedKeyInfo, retries uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), cli.RequestTimeout)
	defer cancel()

	output, _ := json.Marshal(info)
	previousChunks := fmt.Sprintf("%s/chunks/v%d/", key, info.Version-1)
	tx := cli.Client.Txn(ctx).Then(
		clientv3.OpPut(fmt.Sprintf("%s/info", key), string(output)),
		clientv3.OpDelete(previousChunks, clientv3.WithRange(clientv3.GetPrefixRangeEnd(previousChunks))),
	)

	_, err := tx.Commit()
	if err != nil {
		if shouldRetry(err, retries) {
			time.Sleep(cli.RetryInterval)
			return cli.persistVersionChange(key, info, retries-1)
		}
	}

	return err
}

func (cli *EtcdClient) PutChunkedKey(key *ChunkedKeyPayload) error {
	cMaxSize := int64(1024 * 1024)
	keyInfo, _, infoErr := cli.getChunkedKeyInfo(key.Key)
	if infoErr != nil {
		return infoErr
	}

	var version int64
	if keyInfo != nil {
		version = keyInfo.Version
	} else {
		version = 0
	}

	//Cleanup before write in case a previous write attempt aborted in error
	clearErr := cli.DeletePrefix(fmt.Sprintf("%s/chunks/v%d/", key.Key, version+1))
	if clearErr != nil {
		return clearErr
	}

	//Write all the chunks
	chunks := key.Size / cMaxSize
	if (key.Size % cMaxSize) > 0 {
		chunks += 1
	}

	buf := make([]byte, cMaxSize)
	for idx := int64(0); idx < chunks; idx++ {
		cKey := fmt.Sprintf("%s/chunks/v%d/%d", key.Key, version+1, idx)

		if idx < (chunks-1) || (key.Size%cMaxSize) == 0 {
			_, readErr := io.ReadFull(key.Value, buf)
			if readErr != nil {
				return readErr
			}

			putErr := cli.PutKey(cKey, string(buf))
			if putErr != nil {
				return putErr
			}
		} else {
			_, readErr := io.ReadAtLeast(key.Value, buf, int(key.Size%cMaxSize))
			if readErr != nil {
				return readErr
			}

			putErr := cli.PutKey(cKey, string(buf[:key.Size%cMaxSize]))
			if putErr != nil {
				return putErr
			}
		}
	}

	//update chunk info and delete previous version chunks as single transaction
	return cli.persistVersionChange(key.Key, ChunkedKeyInfo{
		Size:    key.Size,
		Count:   chunks,
		Version: version + 1,
	}, cli.Retries)
}

type ChunksReader struct {
	Client   *EtcdClient
	Key      string
	Index    int64
	Buffer   *bytes.Buffer
	Snapshot ChunkedKeySnapshot
}

func (r *ChunksReader) Close() error {
	r.Client = nil
	r.Buffer = nil
	r.Snapshot = ChunkedKeySnapshot{}
	return nil
}

func (r *ChunksReader) Read(p []byte) (n int, err error) {
	unread := r.Buffer.Len()
	if unread > 0 {
		return r.Buffer.Read(p)
	}

	if r.Index == r.Snapshot.Info.Count {
		return 0, io.EOF
	}

	chunkKey := fmt.Sprintf("%s/chunks/v%d/%d", r.Key, r.Snapshot.Info.Version, r.Index)
	kInfo, kExists, kErr := r.Client.GetKey(chunkKey)
	if kErr != nil {
		return 0, kErr
	}
	if !kExists {
		return 0, errors.New(fmt.Sprintf("%s chunk key not found", chunkKey))
	}

	r.Index += 1

	_, wErr := r.Buffer.WriteString(kInfo.Value)
	if wErr != nil {
		return 0, wErr
	}

	return r.Buffer.Read(p)
}

func (cli *EtcdClient) newChunksReader(key string) (*ChunksReader, error) {
	cKeyInfo, revision, infoErr := cli.getChunkedKeyInfo(key)
	if infoErr != nil {
		return nil, infoErr
	}
	if cKeyInfo == nil {
		return nil, errors.New(fmt.Sprintf("%s key doesn't have chunked key info", key))
	}

	var buffer bytes.Buffer
	buffer.Grow(1024 * 1024)
	reader := ChunksReader{
		Client: cli,
		Key:    key,
		Index:  0,
		Buffer: &buffer,
		Snapshot: ChunkedKeySnapshot{
			Info:     *cKeyInfo,
			Revision: revision,
		},
	}

	return &reader, nil
}

func (cli *EtcdClient) GetChunkedKey(key string) (*ChunkedKeyPayload, error) {
	keyInfo, _, infoErr := cli.getChunkedKeyInfo(key)
	if infoErr != nil || keyInfo == nil {
		return nil, infoErr
	}

	reader, rErr := cli.newChunksReader(key)
	if rErr != nil {
		return nil, rErr
	}

	payload := ChunkedKeyPayload{
		Key:   key,
		Value: reader,
		Size:  reader.Snapshot.Info.Size,
	}

	return &payload, nil
}

func (cli *EtcdClient) deleteChunkedKeyWithRetries(key string, retries uint64) error {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	chunksPrefix := fmt.Sprintf("%s/chunks/", key)
	infoKey := fmt.Sprintf("%s/info", key)
	tx := cli.Client.Txn(ctx).Then(
		clientv3.OpDelete(infoKey),
		clientv3.OpDelete(chunksPrefix, clientv3.WithRange(clientv3.GetPrefixRangeEnd(chunksPrefix))),
	)

	_, err := tx.Commit()
	if err != nil {
		if shouldRetry(err, retries) {
			time.Sleep(cli.RetryInterval)
			return cli.deleteChunkedKeyWithRetries(key, retries-1)
		}
	}

	return err
}

func (cli *EtcdClient) DeleteChunkedKey(key string) error {
	return cli.deleteChunkedKeyWithRetries(key, cli.Retries)
}
