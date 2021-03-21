package clienttest;

import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RBuckets;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

public class RedissionTest {
    RedissonClient rd;

    @BeforeClass
    public void setUp() {
        var address = String.format("redis://%s:%d", Utils.getNodeHost(), Utils.getNodePort());

        Config config = new Config();
        config.useClusterServers()
                .setScanInterval(2000)
                .setReadMode(ReadMode.MASTER)
                .addNodeAddress(address);
        this.rd = Redisson.create(config);
    }

    String genKey(String testCase, String key) {
        final var now = new java.util.Date();
        return String.format("redission:%d:%s:%s", now.toInstant().toEpochMilli(), testCase, key);
    }

    @Test
    public void singleKeyCommand() {
        final var key = this.genKey("singleKey", "somekey");
        final var value = "value";
        RBucket<String> bucket = this.rd.getBucket(key);
        bucket.set(value, 60, TimeUnit.SECONDS);
        final var v = bucket.get();
        assertEquals(value, v);
    }

    @Test
    public void multiKeyCommand() {
        // for hashtag,  refers to https://redis.io/topics/cluster-spec
        final var key1 = this.genKey("multikey", "key1:{hashtag}");
        final var key2 = this.genKey("multikey", "key2:{hashtag}");
        final var value1 = "value1";
        final var value2 = "value2";

        RBuckets buckets = this.rd.getBuckets();

        var map = new HashMap<String, String>();
        map.put(key1, value1);
        map.put(key2, value2);
        buckets.set(map);

        Map<String, String> loadedBuckets = buckets.get(key1, key2);
        assertEquals(loadedBuckets.size(), 2);
        assertEquals(loadedBuckets.get(key1), value1);
        assertEquals(loadedBuckets.get(key2), value2);
    }
}
