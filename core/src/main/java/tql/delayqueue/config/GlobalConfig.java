package tql.delayqueue.config;

import tql.delayqueue.utils.Utils;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 */
public class GlobalConfig {
    public static volatile String appUniqueIdentifier = "default:" + Utils.getUUIDByFormatter();
    public static final List<NamespaceConfig> namespaceConfigs = new ArrayList<>();
    public static volatile int noHeartBeatTimeInMillSec = 5000;
    public static volatile int heartbeatIntervalTimeInMillis = 1000;
    public static volatile int pollIntervalTimeInMillSecs = 5 * 60 * 1000;
    public static volatile String partitionAssignerName = "random";
    public static volatile String partitionSelectorName = "hash";
    public static volatile String redisHost = "127.0.0.1";
    public static volatile int redisPort = 6379;
    public static volatile String redisPassword;
    public static volatile int redisDatabase = 0;


}
