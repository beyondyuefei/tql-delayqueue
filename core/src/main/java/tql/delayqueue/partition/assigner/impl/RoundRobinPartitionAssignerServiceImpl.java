package tql.delayqueue.partition.assigner.impl;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import tql.delayqueue.config.NamespaceConfig;
import tql.delayqueue.partition.PartitionWorker;
import tql.delayqueue.partition.assigner.AbstractPartitionAssignerService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 注： 这里是针对每一个的namespace都执行一边RR，那么在 worker数量多于namespace的partitionSize情况下，就会出现多出的worker分配不到partition任务的情况
 * TODO: 待优化, 思路可参考 kafka 的 RR算法 ?
 */
@Slf4j
public class RoundRobinPartitionAssignerServiceImpl extends AbstractPartitionAssignerService{
    @Override
    public Map<String, List<Integer>> assignWorkersPartition(final NamespaceConfig namespaceConfig, final List<PartitionWorker> workers) {
        final Map<String, List<Integer>> workersPartitionMap = initWorkersPartitionMap(workers);
        final Map<String, WeightedRoundRobin> workerWeightMap = initializeWorkerWeightMap(workers);
        for (int partitionNumber = 0; partitionNumber < namespaceConfig.getPartitionSize(); partitionNumber++) {
            final PartitionWorker partitionWorker = selectWorker(workerWeightMap, partitionNumber, workers);
            final List<Integer> workerPartitionList = workersPartitionMap.get(partitionWorker.getWorkerUniqueIdentifier());
            workerPartitionList.add(partitionNumber); // Add the partition number instead of worker number
            workersPartitionMap.put(partitionWorker.getWorkerUniqueIdentifier(), workerPartitionList);
        }

        return workersPartitionMap;
    }

    /**
     * 在加权轮询算法中，totalWeight 变量用于计算所有服务提供者的总权重，而将被选择的 worker 的 current 值减少 totalWeight 是为了确保下次选择时能够按照正确的权重比例进行轮询。
     * totalWeight 的作用
     *   totalWeight 的目的是记录所有服务提供者的权重之和。这是因为我们需要知道总的权重值，才能根据各个服务提供者的权重比例来选择下一个要使用的服务提供者。具体来说，在每次选择服务提供者时，我们希望选择当前累计值（即 current）最大的服务提供者。为了确保选择是基于权重的，我们需要在每次选择后将该服务提供者的 current 减去 totalWeight。
     *
     * 减少 current 的原因
     *   在选择了一个服务提供者之后，我们更新它的 current 值。这是因为每次选择服务提供者时，我们都是基于 current 值的大小来决定的。如果某个服务提供者的 current 值较大，那么它更有可能被选中。但是，由于我们是按照权重进行选择的，所以需要确保每个服务提供者的 current 值反映了它们之间的相对权重比例。
     *
     * 当选择了某个服务提供者后，我们通过减去 totalWeight 来更新它的 current 值。这实际上是模拟了一种“虚拟等待”机制，使得高权重的服务提供者在一段时间内有更多的机会被选中，而低权重的服务提供者则较少被选中。这样的处理确保了服务提供者的选择是公平的，并且遵循了权重配置。
     *
     * 示例说明
     *  假设我们有三个服务提供者 A、B 和 C，它们的权重分别为 1、2 和 3。那么，totalWeight 就是 6。
     *
     *  初始状态下，每个服务提供者的 current 值都为 0。
     *  第一次选择时，A 的 current 值变为 1，B 的 current 值变为 2，C 的 current 值变为 3。
     *  假设 C 被选中，那么它的 current 值会被减少 6（即 totalWeight），变为 -3。
     *  下一次选择时，A 的 current 值变为 1，B 的 current 值变为 2，C 的 current 值变为 0（因为已经减少了 6 并且增加了 3）。这意味着 B 更有可能被选中，因为它有更高的 current 值。
     *
     * 通过这种方式，即使在多次选择之后，每个服务提供者的选择频率也会尽可能地符合它们的权重比例。
     *
     */
    private PartitionWorker selectWorker(final Map<String, WeightedRoundRobin> workerWeightMap, final int partitionNumber, final List<PartitionWorker> partitionWorkers) {
        int totalWeight = 0;
        long maxCurrent = Long.MIN_VALUE;
        WeightedRoundRobin selectedWRR = null;
        String workerUniqueIdentifier = null;
        for (Map.Entry<String, WeightedRoundRobin> weightedRoundRobinEntry : workerWeightMap.entrySet()) {
            long cur = weightedRoundRobinEntry.getValue().increaseCurrent();
            if (cur > maxCurrent) {
                maxCurrent = cur;
                selectedWRR = weightedRoundRobinEntry.getValue();
                workerUniqueIdentifier = weightedRoundRobinEntry.getKey();
            }
            totalWeight += weightedRoundRobinEntry.getValue().getWeight();
        }

        if (selectedWRR != null) {
            selectedWRR.sel(totalWeight);
            for (final PartitionWorker worker : partitionWorkers) {
                if (worker.getWorkerUniqueIdentifier().equals(workerUniqueIdentifier)) {
                    return worker;
                }
            }
        }
        // 兜底，理论上不应该执行到这里
        log.warn("unexpected execute here, RR for select PartitionWorker, workerWeightMap:{}", workerWeightMap);
        return partitionWorkers.get(partitionNumber % partitionWorkers.size());
    }

    private Map<String, WeightedRoundRobin> initializeWorkerWeightMap(final List<PartitionWorker> workers) {
        Map<String, WeightedRoundRobin> workerWeightMap = new HashMap<>();
        for (PartitionWorker worker : workers) {
            WeightedRoundRobin wrr = new WeightedRoundRobin(worker.getWeight());
            workerWeightMap.put(worker.getWorkerUniqueIdentifier(), wrr);
        }
        return workerWeightMap;
    }


    private static class WeightedRoundRobin {
        @Getter
        private int weight;
        // 通过 current 值，来反应权重的比例，初始值为 0
        private final AtomicLong current = new AtomicLong(0);

        public WeightedRoundRobin(int weight) {
            this.weight = weight;
        }

        public void setWeight(int weight) {
            this.weight = weight;
            current.set(0);
        }

        public long increaseCurrent() {
            return current.addAndGet(weight);
        }

        public void sel(int total) {
            current.addAndGet(-1 * total);
        }
    }
}
