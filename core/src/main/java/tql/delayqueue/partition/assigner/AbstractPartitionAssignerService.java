package tql.delayqueue.partition.assigner;

import tql.delayqueue.partition.PartitionWorker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractPartitionAssignerService implements PartitionAssignerService{
    protected Map<String, List<Integer>> initWorkersPartitionMap(final List<PartitionWorker> workers) {
        final Map<String, List<Integer>> workersPartitionMap = new HashMap<>();
        workers.forEach(partitionWorker -> workersPartitionMap.put(partitionWorker.getWorkerUniqueIdentifier(), new ArrayList<>()));
        return workersPartitionMap;
    }
}
