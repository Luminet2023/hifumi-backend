package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultLeaseTTL = 75 * time.Second
	HintChannelName = "sync:hints"
)

var ErrConnectionLimit = errors.New("sync connection limit reached")

type Hint struct {
	OwnerKey           string `json:"ownerKey"`
	BaselineID         string `json:"baselineId"`
	ServerCursor       uint64 `json:"serverCursor"`
	ServerVersion      uint64 `json:"serverVersion"`
	OriginConnectionID string `json:"originConnectionId,omitempty"`
}

type Client struct {
	redis  *redis.Client
	prefix string
}

func New(redisURL, prefix string) (*Client, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse REDIS_URL: %w", err)
	}
	return &Client{redis: redis.NewClient(options), prefix: prefix}, nil
}

func (c *Client) Close() error {
	return c.redis.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	return c.redis.Ping(ctx).Err()
}

var fixedWindowScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('SET', KEYS[1], 1, 'PX', ARGV[2])
  return 0
end
if tonumber(current) >= tonumber(ARGV[1]) then
  local ttl = redis.call('PTTL', KEYS[1])
  if ttl < 1 then ttl = 1000 end
  return ttl
end
redis.call('INCR', KEYS[1])
return 0
`)

func (c *Client) CheckFixedWindow(ctx context.Context, operation, ownerKey string, limit int, window time.Duration) (time.Duration, error) {
	key := c.prefix + "rate:" + operation + ":" + ownerKey
	result, err := fixedWindowScript.Run(ctx, c.redis, []string{key}, limit, window.Milliseconds()).Int64()
	if err != nil {
		return 0, err
	}
	return time.Duration(result) * time.Millisecond, nil
}

var acquireLeaseScript = redis.NewScript(`
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[3]) then
  return 0
end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return 1
`)

func (c *Client) AcquireConnection(ctx context.Context, ownerKey, connectionID string, maxConnections int, ttl time.Duration) error {
	now := time.Now().UnixMilli()
	key := c.prefix + "connections:" + ownerKey
	result, err := acquireLeaseScript.Run(ctx, c.redis, []string{key}, now, now+ttl.Milliseconds(), maxConnections, connectionID, (2 * ttl).Milliseconds()).Int()
	if err != nil {
		return err
	}
	if result != 1 {
		return ErrConnectionLimit
	}
	return nil
}

var refreshLeaseScript = redis.NewScript(`
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return 0
end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
`)

func (c *Client) RefreshConnection(ctx context.Context, ownerKey, connectionID string, ttl time.Duration) (bool, error) {
	key := c.prefix + "connections:" + ownerKey
	result, err := refreshLeaseScript.Run(ctx, c.redis, []string{key}, connectionID, time.Now().Add(ttl).UnixMilli(), (2 * ttl).Milliseconds()).Int()
	return result == 1, err
}

func (c *Client) ReleaseConnection(ctx context.Context, ownerKey, connectionID string) error {
	return c.redis.ZRem(ctx, c.prefix+"connections:"+ownerKey, connectionID).Err()
}

func (c *Client) PublishHint(ctx context.Context, hint Hint) error {
	payload, err := json.Marshal(hint)
	if err != nil {
		return err
	}
	return c.redis.Publish(ctx, c.prefix+HintChannelName, payload).Err()
}

func (c *Client) SubscribeHints(ctx context.Context, onHint func(Hint)) error {
	subscription := c.redis.Subscribe(ctx, c.prefix+HintChannelName)
	defer subscription.Close()
	if _, err := subscription.Receive(ctx); err != nil {
		return err
	}
	channel := subscription.Channel(redis.WithChannelSize(256))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message, ok := <-channel:
			if !ok {
				return fmt.Errorf("redis hint subscription closed")
			}
			var hint Hint
			if json.Unmarshal([]byte(message.Payload), &hint) == nil && hint.OwnerKey != "" && hint.BaselineID != "" {
				onHint(hint)
			}
		}
	}
}

func (c *Client) DebugKey(operation, ownerKey string) string {
	return c.prefix + operation + ":" + ownerKey
}

func RetryAfterSeconds(delay time.Duration) string {
	seconds := (delay + time.Second - 1) / time.Second
	if seconds < 1 {
		seconds = 1
	}
	return strconv.FormatInt(int64(seconds), 10)
}
