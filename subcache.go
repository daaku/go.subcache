// Package subcache provides a way to split a single bytecache into multiple
// subcaches. It allows for a fixed key prefix and collection stats.
package subcache

import (
	"errors"
	"strings"
	"time"
)

var errEmptyPrefix = errors.New("subcache: empty prefix")

// The backend as well as the interface each subcache provides.
type ByteCache interface {
	Store(key string, value []byte, timeout time.Duration) error
	Get(key string) ([]byte, error)
}

// Used to indicate the type of operation for Stats purposes.
type op string

const (
	OpStore = op("store")
	OpGet   = op("get")
)

// This information is provided to the stats handler if one is configured.
type Stats struct {
	Client   *Client       // the Client handling the request
	Op       op            // operation type such as OpStore or OpGet
	Key      string        // key string including the prefix
	Value    []byte        // the value, if one was found
	Error    error         // error from the underlying backend if any
	Duration time.Duration // duration it took for the operation
}

// Client for a subcache.
type Client struct {
	ByteCache ByteCache
	Prefix    string
	Stats     func(s *Stats)
}

func (c *Client) Store(key string, value []byte, timeout time.Duration) error {
	if c.Prefix == "" {
		return errEmptyPrefix
	}

	key = strings.Join([]string{c.Prefix, key}, ":")
	stats := Stats{
		Client: c,
		Op:     OpStore,
		Key:    key,
		Value:  value,
	}
	defer c.Stats(&stats)

	start := time.Now()
	err := c.ByteCache.Store(key, value, timeout)
	stats.Duration = time.Since(start)
	stats.Error = err
	return err
}

func (c *Client) Get(key string) ([]byte, error) {
	if c.Prefix == "" {
		return nil, errEmptyPrefix
	}

	key = strings.Join([]string{c.Prefix, key}, ":")
	stats := Stats{
		Client: c,
		Op:     OpGet,
		Key:    key,
	}
	defer c.Stats(&stats)

	start := time.Now()
	value, err := c.ByteCache.Get(key)
	stats.Duration = time.Since(start)
	stats.Value = value
	stats.Error = err
	return value, err
}
