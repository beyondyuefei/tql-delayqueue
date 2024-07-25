package tql.delayqueue.partition.assigner.impl;

import tql.delayqueue.config.NamespaceConfig;
import tql.delayqueue.partition.PartitionWorker;
import tql.delayqueue.partition.assigner.PartitionAssignerService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class RandomPartitionAssignerServiceImpl implements PartitionAssignerService {
    /**
     *
     * @param namespaceConfig
     * @param workers
     * @return key: PartitionWorker#workerUniqueIdentitfer 、 value: partition list
     */
    @Override
    public Map<String, List<Integer>> assignWorkersPartition(NamespaceConfig namespaceConfig, List<PartitionWorker> workers) {
        final Map<String, List<Integer>> workersPartition = new HashMap<>();
        for (int i = 1;i <= namespaceConfig.getPartitionSize();i++) {
            final PartitionWorker partitionWorker = workers.get(ThreadLocalRandom.current().nextInt(workers.size()));
            final String workerUniqueIdentifier = partitionWorker.getWorkerUniqueIdentifier();
            List<Integer> partitions = workersPartition.get(workerUniqueIdentifier);
            if (partitions == null) {
                partitions = new ArrayList<>();
            }
            partitions.add(i);
            workersPartition.put(workerUniqueIdentifier, partitions);
        }
        return workersPartition;
    }
}
