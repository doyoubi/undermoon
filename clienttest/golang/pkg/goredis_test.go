package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

var clusterClient *redis.ClusterClient
var ctx context.Context = context.Background()

const expireTime = time.Minute

func genKey(testcase, key string) string {
	return fmt.Sprintf("goredis:%d:%s:%s", time.Now().UnixNano(), testcase, key)
}

func TestMain(m *testing.M) {
	host, port := getNodeAddress()
	clusterClient = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{fmt.Sprintf("%s:%s", host, port)},
	})
	os.Exit(m.Run())
}

func TestSingleKeyCommand(t *testing.T) {
	assert := assert.New(t)

	key := genKey("singlekey", "key")
	const value = "singlevalue"

	_, err := clusterClient.Set(ctx, key, value, expireTime).Result()
	assert.NoError(err)

	v, err := clusterClient.Get(ctx, key).Result()
	assert.NoError(err)
	assert.Equal(value, v)
}

func TestMultiKeyCommand(t *testing.T) {
	assert := assert.New(t)

	key1 := genKey("multikey", "key1:{hashtag}")
	key2 := genKey("multikey", "key2:{hashtag}")
	const value1 = "value1"
	const value2 = "value2"

	_, err := clusterClient.MSet(ctx, key1, value1, key2, value2).Result()
	assert.NoError(err)

	values, err := clusterClient.MGet(ctx, key1, key2).Result()
	assert.NoError(err)
	assert.Equal(value1, values[0])
	assert.Equal(value2, values[1])

	count, err := clusterClient.Del(ctx, key1, key2).Result()
	assert.NoError(err)
	assert.Equal(int64(2), count)

	count, err = clusterClient.Exists(ctx, key1, key2).Result()
	assert.NoError(err)
	assert.Equal(int64(0), count)
}

func TestBrpoplpush(t *testing.T) {
	assert := assert.New(t)

	key1 := genKey("blocking", "brpoplpush_key1:{hashtag}")
	key2 := genKey("blocking", "brpoplpush_key2:{hashtag}")
	const value = "listvalue"

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		res, err := clusterClient.BRPopLPush(ctx, key1, key2, time.Minute).Result()
		assert.NoError(err)
		assert.Equal(value, res)
		return nil
	})

	time.Sleep(time.Second)
	l, err := clusterClient.RPush(ctx, key1, value).Result()
	assert.NoError(err)
	assert.Equal(int64(1), l)

	group.Wait()
}

func TestBlpop(t *testing.T) {
	testListBlockingCommandHelper(t, func(ctx context.Context, key1, key2 string) ([]string, error) {
		return clusterClient.BLPop(ctx, time.Minute, key1, key2).Result()
	})
}

func TestBrpop(t *testing.T) {
	testListBlockingCommandHelper(t, func(ctx context.Context, key1, key2 string) ([]string, error) {
		return clusterClient.BRPop(ctx, time.Minute, key1, key2).Result()
	})
}

type ListCmdFunc func(context.Context, string, string) ([]string, error)

func testListBlockingCommandHelper(t *testing.T, cmdFunc ListCmdFunc) {
	assert := assert.New(t)

	key1 := genKey("blocking", "listkey1:{hashtag}")
	key2 := genKey("blocking", "listkey2:{hashtag}")
	const value = "listvalue"

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		res, err := cmdFunc(ctx, key1, key2)
		assert.NoError(err)
		assert.Equal(2, len(res))
		assert.Equal(key2, res[0])
		assert.Equal(value, res[1])
		return nil
	})

	time.Sleep(time.Second)
	l, err := clusterClient.RPush(ctx, key2, value).Result()
	assert.NoError(err)
	assert.Equal(int64(1), l)

	group.Wait()
}

func TestBzpopmin(t *testing.T) {
	testZsetBlockingCommandHelper(t, func(ctx context.Context, key1, key2 string) (*redis.ZWithKey, error) {
		return clusterClient.BZPopMin(ctx, time.Minute, key1, key2).Result()
	})
}

func TestBzpipmax(t *testing.T) {
	testZsetBlockingCommandHelper(t, func(ctx context.Context, key1, key2 string) (*redis.ZWithKey, error) {
		return clusterClient.BZPopMax(ctx, time.Minute, key1, key2).Result()
	})
}

type ZsetCmdFunc func(context.Context, string, string) (*redis.ZWithKey, error)

func testZsetBlockingCommandHelper(t *testing.T, cmdFunc ZsetCmdFunc) {
	assert := assert.New(t)

	key1 := genKey("blocking", "zsetkey1:{hashtag}")
	key2 := genKey("blocking", "zsetkey2:{hashtag}")
	const member = "zsetmember"
	const score = 0.0

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		res, err := cmdFunc(ctx, key1, key2)
		assert.NoError(err)
		assert.NotNil(res)
		assert.Equal(key2, res.Key)
		assert.Equal(member, res.Member)
		assert.Equal(score, res.Score)
		return nil
	})

	time.Sleep(time.Second)
	l, err := clusterClient.ZAdd(ctx, key2, &redis.Z{
		Score:  score,
		Member: member,
	}).Result()
	assert.NoError(err)
	assert.Equal(int64(1), l)

	group.Wait()
}

func TestSingleKeyEval(t *testing.T) {
	assert := assert.New(t)

	key := genKey("singlekey_eval", "key")
	const arg = "arg"

	const script = "return {KEYS[1],ARGV[1]}"
	vals, err := clusterClient.Eval(ctx, script, []string{key}, arg).Result()
	assert.NoError(err)

	strs := vals.([]interface{})
	assert.Equal(2, len(strs))
	assert.Equal(key, strs[0].(string))
	assert.Equal(arg, strs[1].(string))
}

func TestMultiKeyEval(t *testing.T) {
	assert := assert.New(t)

	key1 := genKey("multikey-eval", "key1:{hashtag}")
	key2 := genKey("multikey-eval", "key2:{hashtag}")
	const arg1 = "arg1"
	const arg2 = "arg2"

	const script = "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}"
	vals, err := clusterClient.Eval(ctx, script, []string{key1, key2}, arg1, arg2).Result()
	assert.NoError(err)

	strs := vals.([]interface{})
	assert.Equal(4, len(strs))
	assert.Equal(key1, strs[0].(string))
	assert.Equal(key2, strs[1].(string))
	assert.Equal(arg1, strs[2].(string))
	assert.Equal(arg2, strs[3].(string))
}
