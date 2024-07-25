package tql.delayqueue.redisson;

import tql.delayqueue.config.GlobalConfig;
import lombok.extern.slf4j.Slf4j;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

@Slf4j
public class RedissonClientFactory {
    private static RedissonClient redissonClient;
    public static RedissonClient getInstance() {
        if (redissonClient == null) {
            final Config config = new Config();
            final String url = String.format("redis://%s:%s", GlobalConfig.redisHost, GlobalConfig.redisPort);
            config.useSingleServer()
                    .setAddress(url)
                    .setPingConnectionInterval(0)//禁用心跳检测
                    .setPassword(GlobalConfig.redisPassword)
                    .setDatabase(GlobalConfig.redisDatabase)
                    .setPingConnectionInterval(2000);
            config.setLockWatchdogTimeout(10000L);
            redissonClient = Redisson.create(config);
            log.info("init redisson client success, redis url:{}", url);
        }

        return redissonClient;
    }

    public static void shutdown() {
        if (redissonClient != null) {
            redissonClient.shutdown();
            log.info("shutdown redisson client success");
        }
    }
}
