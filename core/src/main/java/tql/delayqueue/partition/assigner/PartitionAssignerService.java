package tql.delayqueue.partition.assigner;

import tql.delayqueue.config.NamespaceConfig;
import tql.delayqueue.partition.PartitionWorker;

import java.util.List;
import java.util.Map;

/**
 * 为 {@link NamespaceConfig#namespace} 分配每个{@link PartitionWorker}负责的Partitions，具体有不同的LoadBalance算法实现
 */
public interface PartitionAssignerService {
    /**
     *
     * @param namespaceConfig
     * @param workers
     * @return key: workerUniqueIdentifier, value: partitions for this worker
     */
    Map<String, List<Integer>> assignWorkersPartition(final NamespaceConfig namespaceConfig, final List<PartitionWorker> workers);
}
