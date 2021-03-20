package clienttest;

import io.lettuce.core.KeyValue;
import io.lettuce.core.cluster.RedisClusterClient;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.function.BiFunction;

import static org.testng.Assert.assertEquals;

public class LettuceTest {
    RedisClusterClient lc;

    @BeforeClass
    public void setUp() {
        var address = String.format("redis://%s:%d", Utils.getNodeHost(), Utils.getNodePort());
        this.lc = RedisClusterClient.create(address);
    }

    String genKey(String testCase, String key) {
        final var now = new java.util.Date();
        return String.format("lettuce:%d:%s:%s", now.toInstant().toEpochMilli(), testCase, key);
    }

    @Test
    public void singleKeyCommand() {
        final var key = this.genKey("singleKey", "somekey");
        final var value = "value";
        final var conn = this.lc.connect();
        final var syncCmd = conn.sync();
        syncCmd.setex(key, 60, value);
        final var v = syncCmd.get(key);
        assertEquals(value, v);
    }

    @Test
    public void multiKeyCommand() {
        // for hashtag,  refers to https://redis.io/topics/cluster-spec
        final var key1 = this.genKey("multikey", "key1:{hashtag}");
        final var key2 = this.genKey("multikey", "key2:{hashtag}");
        final var value1 = "value1";
        final var value2 = "value2";

        final var conn = this.lc.connect();
        final var syncCmd = conn.sync();

        var map = new HashMap<String, String>();
        map.put(key1, value1);
        map.put(key2, value2);
        syncCmd.mset(map);
        final var values = syncCmd.mget(key1, key2);
        assertEquals(values.size(), 2);
        assertEquals(values.get(0).getValue(), value1);
        assertEquals(values.get(1).getValue(), value2);
    }

    @Test
    public void brpoplpush() throws InterruptedException {
        final var key1 = this.genKey("blocking", "brpoplpush_key1:{hashtag}");
        final var key2 = this.genKey("blocking", "brpoplpush_key2:{hashtag}");
        final var value = "listvalue";

        Runnable blockingCmd = () -> {
            final var res = this.lc.connect().sync().brpoplpush(60, key1, key2);
            assertEquals(res, value);
        };
        var thread = new Thread(blockingCmd);
        thread.start();

        Thread.sleep(1000);
        var listLen = this.lc.connect().sync().rpush(key1, value);
        assertEquals(listLen, Long.valueOf(1));

        thread.join();
    }

    @Test
    void blpop() throws InterruptedException {
        listBlockingCommandHelper((key1, key2) -> this.lc.connect().sync().blpop(60, key1, key2));
    }

    @Test
    void brpop() throws InterruptedException {
        listBlockingCommandHelper((key1, key2) -> this.lc.connect().sync().brpop(60, key1, key2));
    }

    void listBlockingCommandHelper(BiFunction<String, String, KeyValue<String, String>> cmdFunc) throws InterruptedException {
        final var key1 = this.genKey("blocking", "key1:{hashtag}");
        final var key2 = this.genKey("blocking", "key2:{hashtag}");
        final var value = "listvalue";

        Runnable blockingCmd = () -> {
            final var res = cmdFunc.apply(key1, key2);
            assertEquals(res.getKey(), key2);
            assertEquals(res.getValue(), value);
        };
        var thread = new Thread(blockingCmd);
        thread.start();

        Thread.sleep(1000);
        var listLen = this.lc.connect().sync().rpush(key2, value);
        assertEquals(listLen, Long.valueOf(1));

        thread.join();
    }
}
