package tql.delayqueue.partition.assigner;

import tql.delayqueue.partition.assigner.impl.RandomPartitionAssignerServiceImpl;
import tql.delayqueue.partition.assigner.impl.RoundRobinPartitionAssignerServiceImpl;
import tql.delayqueue.utils.TQLDelayQueueException;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO: 需要支持用户的自定义扩展实现
 */
@Slf4j
public class PartitionAssignerFactory {
    private static final ConcurrentHashMap<String, PartitionAssignerService> partitionAssignerMap = new ConcurrentHashMap<>();

    public static PartitionAssignerService create(final String name) {
        if (name.equals("random")) {
            return partitionAssignerMap.computeIfAbsent(name, notUsed -> new RoundRobinPartitionAssignerServiceImpl());
        } else if (name.equals("round-robin")) {
            return partitionAssignerMap.computeIfAbsent(name, notUsed -> new RandomPartitionAssignerServiceImpl());
        } else {
            final String errorMsg = "unknown PartitionAssignerService name: " + name;
            log.error(errorMsg);
            throw new TQLDelayQueueException(errorMsg);
        }
    }
}
