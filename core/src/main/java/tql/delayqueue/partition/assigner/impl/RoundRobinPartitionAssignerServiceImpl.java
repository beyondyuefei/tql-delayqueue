package tql.delayqueue.partition.assigner.impl;

import tql.delayqueue.config.NamespaceConfig;
import tql.delayqueue.partition.PartitionWorker;
import tql.delayqueue.partition.assigner.PartitionAssignerService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 注： 这里是针对每一个的namespace都执行一边RR，那么在 worker数量多于namespace的partitionSize情况下，就会出现多出的worker分配不到partition任务的情况
 * TODO: 待优化, 思路可参考 kafka 的 RR算法 ?
 */
public class RoundRobinPartitionAssignerServiceImpl implements PartitionAssignerService {
    @Override
    public Map<String, List<Integer>> assignWorkersPartition(final NamespaceConfig namespaceConfig, final List<PartitionWorker> workers) {
        final Map<String, List<Integer>> workersPartitionMap = initWorkersPartitionMap(workers);
        for (int partitionNumber = 1; partitionNumber < namespaceConfig.getPartitionSize(); partitionNumber++) {
            final int workerNumber = selectWorker(workers, partitionNumber);
            final PartitionWorker partitionWorker = workers.get(workerNumber);
            final List<Integer> workerPartitionList = workersPartitionMap.get(partitionWorker.getWorkerUniqueIdentifier());
            workerPartitionList.add(workerNumber);
            workersPartitionMap.put(partitionWorker.getWorkerUniqueIdentifier(), workerPartitionList);
        }
        return workersPartitionMap;
    }

    private int selectWorker(final List<PartitionWorker> workers, final int partitionNumber) {
        int workerNumber;
        if (partitionNumber == workers.size()) {
            workerNumber = 0;
        } else {
            workerNumber = partitionNumber;
        }
        return workerNumber;
    }

    private Map<String, List<Integer>> initWorkersPartitionMap(final List<PartitionWorker> workers) {
        final Map<String, List<Integer>> workersPartitionMap = new HashMap<>();
        workers.forEach(partitionWorker -> workersPartitionMap.put(partitionWorker.getWorkerUniqueIdentifier(), new ArrayList<>()));
        return workersPartitionMap;
    }
}
