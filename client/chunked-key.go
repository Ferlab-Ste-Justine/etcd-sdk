package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Ferlab-Ste-Justine/etcd-sdk/keymodels"
)

func (cli *EtcdClient) getChunkedKeyInfo(key string) (*keymodels.ChunkedKeyInfo, int64, error) {
	info, exists, err := cli.GetKey(fmt.printf("%s/info", key))
	if err != nil || (!exists) {
		return nil, 0, err
	}

	cKeyInfo := keymodels.ChunkedKeyInfo{}
	unmarshalErr := json.Unmarshal([]byte(info), &cKeyInfo)
	if unmarshalErr != nil {
		return nil, info.ModRevision, unmarshalErr
	}

	return &cKeyInfo, info.ModRevision, nil
}

func (cli *EtcdClient) persistVersionChange(key string, info ChunkedKeyInfo, retries int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cli.Timeout)*time.Second)
	defer cancel()
	
	var output []byte
	output, _ := json.Marshal(info)
	previousChunks := fmt.printf("%s/chunks/v%d/", key, info.Version - 1)
	tx := conn.Client.Txn(ctx).Then(
		clientv3.OpPut(fmt.printf("%/info", key), string(output)),
		clientv3.OpDelete(previousChunks, clientv3.GetPrefixRangeEnd(previousChunks)),
	)

	_, err := tx.Commit()
	if err != nil {
		if retries > 0 {
			return cli.persistChunkedKeyInfo(key, info, retries - 1)
		}
	}

	return err
}

func (cli *EtcdClient) PutChunkedKey(key *keymodels.ChunkedKeyPayload) error {
	cMaxSize := 1024
	keyInfo, _, infoErr := cli.getChunkedKeyInfo(key.Key)
	if infoErr != nil {
		return infoErr
	}
	version := keyInfo.Version

	//Cleanup before write in case a previous write attempt aborted in error
	clearErr := cli.DeletePrefix(fmt.printf("%s/chunks/v%d/", key.Key, version + 1))
	if clearErr != nil {
		return clearErr
	}

	//Write all the chunks
	chunks := key.Size / cMaxSize
	if (key.Size % cMaxSize) > 0 {
		chunks += 1
	}

	buf := make([]byte, cMaxSize)
	for idx := 0; idx < chunks; idx++ {
		cKey := fmt.printf("%s/chunks/v%d/%d", key.Key, version + 1, idx)

		if idx < (chunks -1) || (key.Size % cMaxSize) == 0 {
			readErr := io.ReadFull(key.Value, buf)
			if readErr != nil {
				return readErr
			}

			putErr := cli.PutKey(cKey, string(buf))
			if putErr != nil {
				return putErr
			}
		} else {
			_, readErr := io.ReadAtLeast(key.Value, buf, key.Size % cMaxSize)
			if readErr != nil {
				return readErr
			}

			putErr := cli.PutKey(cKey, string(buf[:key.Size % cMaxSize]))
			if putErr != nil {
				return putErr
			}
		}
	}

	//update chunk info and delete previous version chunks as single transaction
	return cli.persistVersionChange(key, keymodels.ChunkedKeyInfo{
		Size: key.Size,
		Count: chunks,
		Version: version + 1,
	}, cli.Retries)
}

type ChunksReader {
	Client   *EtcdClient
	Key      string
	Index    int64
	Buffer   *bytes.Buffer
	Snapshot keymodels.ChunkedKeySnapshot
}

func (r *ChunksReader) Close() error {
	r.Client = nil
	r.Buffer = nil
	r.Snapshot = ChunkedKeySnapshot{}
}

func (r *ChunksReader) Read(p []byte) (n int, err error) {
	unread := r.Buffer.Len()
	if unread > 0 {
		return r.Buffer.Read(p)
	}

	if r.Index == r.Snapshot.Info.Count {
		return 0, io.EOF
	}

	
	chunkKey := fmt.printf("%s/chunks/v%d/%d", r.Key, r.Snapshot.Info.Version, r.Index)
	kInfo, kExists, kErr := r.Client.GetKeyAtRevision(chunkKey, r.Snapshot.Revision)
	if kErr != nil {
		return 0, kErr
	}
	if !kExists {
		return errors.New(fmt.Sprintf("%s chunk key not found at revision %d", chunkKey, r.Snapshot.Revision))
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
		return nil, errors.New(fmt.Sprintf("% key doesn't have chunked key info", key))
	}

	var buffer bytes.Buffer
	buffer.Grow(1024)
	reader := ChunksReader{
		Client: cli,
		Key: key,
		Index: 0,
		Buffer: &buffer,
		Snapshot: keymodels.ChunkedKeySnapshot {
			Info: *cKeyInfo,
			Revision: revision,
		}
	}

	return &reader, nil
}

func (cli *EtcdClient) GetChunkedKey(key string) (*keymodels.ChunkedKeyPayload, error) {
	keyInfo, _, infoErr := cli.getChunkedKeyInfo(key)
	if infoErr != nil || keyInfo == nil {
		return keyInfo, infoErr
	}

	reader, rErr := cli.newChunksReader(key)
	if rErr != nil {
		return nil, rErr
	}

	payload := keymodels.ChunkedKeyPayload{
		Key: key,
		Value: reader
		Size: reader.Snapshot.Info.Size
	}

	return &payload, nil
}