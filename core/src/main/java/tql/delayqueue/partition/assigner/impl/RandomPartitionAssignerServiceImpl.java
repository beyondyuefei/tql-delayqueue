package tql.delayqueue.partition.assigner.impl;

import tql.delayqueue.config.NamespaceConfig;
import tql.delayqueue.partition.PartitionWorker;
import tql.delayqueue.partition.assigner.AbstractPartitionAssignerService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class RandomPartitionAssignerServiceImpl extends AbstractPartitionAssignerService {
    /**
     *
     * @param namespaceConfig
     * @param workers
     * @return key: PartitionWorker#workerUniqueIdentitfer 、 value: partition list
     */
    @Override
    public Map<String, List<Integer>> assignWorkersPartition(NamespaceConfig namespaceConfig, List<PartitionWorker> workers) {
        final Map<String, List<Integer>> workersPartitionMap = initWorkersPartitionMap(workers);
        for (int i = 0;i < namespaceConfig.getPartitionSize();i++) {
            final PartitionWorker partitionWorker = workers.get(ThreadLocalRandom.current().nextInt(workers.size()));
            final String workerUniqueIdentifier = partitionWorker.getWorkerUniqueIdentifier();
            final List<Integer> workerPartitionList = workersPartitionMap.get(workerUniqueIdentifier);
            workerPartitionList.add(i);
            workersPartitionMap.put(workerUniqueIdentifier, workerPartitionList);
        }
        return workersPartitionMap;
    }
}
