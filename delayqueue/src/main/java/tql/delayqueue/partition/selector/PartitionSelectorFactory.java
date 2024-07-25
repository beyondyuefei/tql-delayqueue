package tql.delayqueue.partition.selector;

import tql.delayqueue.partition.selector.impl.HashPartitionSelectorServiceImpl;
import tql.delayqueue.utils.TQLDelayQueueException;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
/**
 * TODO: 需要支持用户的自定义扩展实现
 */
@Slf4j
public class PartitionSelectorFactory {
    private static final ConcurrentHashMap<String, PartitionSelectorService> partitionSelectorMap = new ConcurrentHashMap<>();

    public static PartitionSelectorService create(final String name) {
        if (name.equals("hash")) {
            return partitionSelectorMap.computeIfAbsent(name, notUsed -> new HashPartitionSelectorServiceImpl());
        } else {
            final String errorMsg = "unknown PartitionSelectorService name: " + name;
            log.error(errorMsg);
            throw new TQLDelayQueueException(errorMsg);
        }
    }
}
