package tql.delayqueue;

import tql.delayqueue.partition.PartitionMaster;
import tql.delayqueue.partition.PartitionWorker;
import tql.delayqueue.redisson.RedissonClientFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class TQLDelayQueueLifecycle {
    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private static final PartitionMaster partitionMaster = new PartitionMaster();
    private static final PartitionWorker partitionWorker = new PartitionWorker();

    public static void start() {
        if (initialized.compareAndSet(false, true)) {
            partitionMaster.init();
            partitionWorker.init();
            log.info("init TQL-DelayQueue success");
        }
    }

    public static void stop() {
        final boolean initialed = initialized.get();
        if (initialed) {
            partitionMaster.shutdown();
            partitionWorker.shutdown();
            RedissonClientFactory.shutdown();
            log.info("shutdown TQL-DelayQueue success");
        }
    }
}
