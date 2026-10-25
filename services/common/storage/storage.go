// Package storage is object storage: an S3-compatible store (SeaweedFS) for
// instrument data.
//
// A thin wrapper so you do not have to set up a client. Every call can fail:
// the store can be slow or down.
package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// RequestTimeout bounds every call, so a store that is down fails fast.
const RequestTimeout = 5 * time.Second

type Object struct {
	Key  string
	Size int64
}

type Storage struct {
	Bucket string
	client *minio.Client
}

// New connects to endpoint, such as http://storage:8333. It does not touch the
// network until the first call.
func New(endpoint, bucket string) (*Storage, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("storage endpoint %q: %w", endpoint, err)
	}
	transport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
		ResponseHeaderTimeout: RequestTimeout,
	}
	client, err := minio.New(u.Host, &minio.Options{
		// SeaweedFS runs here without authentication. It refused minio-go's
		// signed uploads, so this client sends unsigned requests.
		Creds:        credentials.NewStatic("", "", "", credentials.SignatureAnonymous),
		Secure:       u.Scheme == "https",
		Region:       "us-east-1",
		BucketLookup: minio.BucketLookupPath,
		Transport:    transport,
		MaxRetries:   1,
	})
	if err != nil {
		return nil, fmt.Errorf("storage client: %w", err)
	}
	return &Storage{Bucket: bucket, client: client}, nil
}

// Ref is the reference for a key, in the form the API reports: s3://bucket/key.
func (s *Storage) Ref(key string) string { return "s3://" + s.Bucket + "/" + key }

func (s *Storage) EnsureBucket(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()
	exists, err := s.client.BucketExists(ctx, s.Bucket)
	if err != nil {
		return fmt.Errorf("check bucket %s: %w", s.Bucket, err)
	}
	if exists {
		return nil
	}
	if err := s.client.MakeBucket(ctx, s.Bucket, minio.MakeBucketOptions{}); err != nil {
		code := minio.ToErrorResponse(err).Code
		if code == "BucketAlreadyOwnedByYou" || code == "BucketAlreadyExists" {
			return nil
		}
		return fmt.Errorf("create bucket %s: %w", s.Bucket, err)
	}
	return nil
}

// Put stores data under key, replacing anything already there. It returns the ref.
func (s *Storage) Put(ctx context.Context, key string, data []byte) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()
	_, err := s.client.PutObject(ctx, s.Bucket, key, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "text/csv"})
	if err != nil {
		return "", fmt.Errorf("put %s: %w", key, err)
	}
	return s.Ref(key), nil
}

func (s *Storage) Get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()
	obj, err := s.client.GetObject(ctx, s.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	return data, nil
}

func (s *Storage) Exists(ctx context.Context, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()
	_, err := s.client.StatObject(ctx, s.Bucket, key, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if code := minio.ToErrorResponse(err).Code; code == "NoSuchKey" || code == "NotFound" {
		return false, nil
	}
	return false, fmt.Errorf("stat %s: %w", key, err)
}

func (s *Storage) Delete(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()
	if err := s.client.RemoveObject(ctx, s.Bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

func (s *Storage) List(ctx context.Context, prefix string) ([]Object, error) {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()
	var out []Object
	for obj := range s.client.ListObjects(ctx, s.Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("list %q: %w", prefix, obj.Err)
		}
		out = append(out, Object{Key: obj.Key, Size: obj.Size})
	}
	return out, nil
}
