package tql.delayqueue;

import tql.delayqueue.config.GlobalConfig;
import tql.delayqueue.config.NamespaceConfig;
import tql.delayqueue.partition.selector.PartitionSelectorFactory;
import tql.delayqueue.partition.selector.PartitionSelectorService;
import tql.delayqueue.redisson.RedissonClientFactory;
import tql.delayqueue.utils.TQLDelayQueueException;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBlockingDeque;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.springframework.lang.NonNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @Description
 */
@Slf4j
class RedissonTQLExecuteServiceImpl implements TQLExecuteService {
    private final RedissonClient redissonClient = RedissonClientFactory.getInstance();
    private final PartitionSelectorService partitionSelectorService;
    private final ConcurrentHashMap<String, NamespaceConfig> namespaceConfigs = new ConcurrentHashMap<>();

    public RedissonTQLExecuteServiceImpl() {
        partitionSelectorService = PartitionSelectorFactory.create(GlobalConfig.partitionSelectorName);
        GlobalConfig.namespaceConfigs.forEach(namespaceConfig -> namespaceConfigs.put(namespaceConfig.getNamespace(), namespaceConfig));
    }

    @Override
    public <T> void executeWithFixedDelay(@NonNull final DelayQueueElement<T> delayQueueElement, @NonNull final long delaySeconds) {
        final String namespace = delayQueueElement.getNamespace();
        final NamespaceConfig namespaceConfig = namespaceConfigs.get(namespace);
        if (namespaceConfig == null) {
            throw new IllegalArgumentException("NamespaceConfig not found for namespace: " + namespace);
        }
        final int partitionIndex = partitionSelectorService.selectPartition(delayQueueElement.getKey(), namespaceConfig);
        final String partitionName = GlobalConfig.appUniqueIdentifier + "_" + namespace + "_" + partitionIndex;
        addDelayQueue(delayQueueElement, delaySeconds, partitionName);
    }

    private <T> void addDelayQueue(@NonNull final DelayQueueElement<T> delayQueueElement, @NonNull final long delaySeconds, @NonNull final String partitionName) {
        try {
            final RBlockingDeque<T> blockingDeque = redissonClient.getBlockingDeque(partitionName);
            final RDelayedQueue<T> delayedQueue = redissonClient.getDelayedQueue(blockingDeque);
            delayedQueue.offer(delayQueueElement.getData(), delaySeconds, TimeUnit.SECONDS);
            log.debug("add delayqueue success, partitionName:{}, value:{}", partitionName, delayQueueElement.getData());
        } catch (Exception e) {
            final String errorMsg = String.format("add TQL-DelayQueue error, namespace:%s, partitionName:%s", delayQueueElement.getNamespace(), partitionName);
            log.error(errorMsg);
            throw new TQLDelayQueueException(errorMsg, e);
        }
    }
}
