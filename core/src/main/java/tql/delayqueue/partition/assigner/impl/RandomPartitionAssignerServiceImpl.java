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
        final Map<String, List<Integer>> workersPartitionMap = initWorkersPartitionMap(workers);
        for (int i = 1;i <= namespaceConfig.getPartitionSize();i++) {
            final PartitionWorker partitionWorker = workers.get(ThreadLocalRandom.current().nextInt(workers.size()));
            final String workerUniqueIdentifier = partitionWorker.getWorkerUniqueIdentifier();
            final List<Integer> workerPartitionList = workersPartitionMap.get(workerUniqueIdentifier);
            workerPartitionList.add(i);
            workersPartitionMap.put(workerUniqueIdentifier, workerPartitionList);
        }
        return workersPartitionMap;
    }

    private Map<String, List<Integer>> initWorkersPartitionMap(final List<PartitionWorker> workers) {
        final Map<String, List<Integer>> workersPartitionMap = new HashMap<>();
        workers.forEach(partitionWorker -> workersPartitionMap.put(partitionWorker.getWorkerUniqueIdentifier(), new ArrayList<>()));
        return workersPartitionMap;
    }
}
