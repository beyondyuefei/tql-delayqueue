package tql.delayqueue.partition.assigner;

import tql.delayqueue.config.NamespaceConfig;
import tql.delayqueue.partition.PartitionWorker;

import java.util.List;
import java.util.Map;

/**
 * @Description
 */
public interface PartitionAssignerService {
    Map<String, List<Integer>> assignWorkersPartition(final NamespaceConfig namespaceConfig, final List<PartitionWorker> workers);
}
