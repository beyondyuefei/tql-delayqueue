package tql.delayqueue.partition;

import tql.delayqueue.callback.CallbackManager;
import tql.delayqueue.config.GlobalConfig;
import tql.delayqueue.config.NamespaceConfig;
import tql.delayqueue.redisson.RedissonClientFactory;
import tql.delayqueue.utils.ConcurrentHashSet;
import tql.delayqueue.utils.Utils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RList;
import org.redisson.api.RedissonClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Description
 */
@Slf4j
@Getter
public class PartitionWorker {
    private  String appUniqueIdentifier;
    private  String workerUniqueIdentifier;
    @Setter
    private volatile long lastHeartbeatTimeInMillis;
    @Setter
    private volatile long lastPollTimeInMillSecs;
    private transient  long heartbeatIntervalTimeInMillis;
    private transient  long pollIntervalTimeInMillSecs;
    private ExecutorService partitionSpecialBossExecutor;
    /**
     * key: namespace; value: 每次并发执行数，如果设置为1 (默认值)，则为顺序执行
     */
    private  ConcurrentHashMap<String, Integer> executeBizBatchSize;
    private final ConcurrentHashSet<String> currentExecutingPartitionBossThread = new ConcurrentHashSet<>();
    private ConcurrentHashMap<String, List<Integer>> latestAssignedNamespacePartitions = new ConcurrentHashMap<>();
    private  Thread heartbeatThread;
    private  Thread pollThread;
    private final RedissonClient redissonClient = RedissonClientFactory.getInstance();
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    public void init() {
        if (initialized.compareAndSet(false, true)) {
            this.appUniqueIdentifier = GlobalConfig.appUniqueIdentifier;
            this.workerUniqueIdentifier = Utils.getLocalAddress().getHostAddress() + "_" + Utils.getUUIDByFormatter();
            this.heartbeatIntervalTimeInMillis = GlobalConfig.heartbeatIntervalTimeInMillis;
            this.pollIntervalTimeInMillSecs = GlobalConfig.pollIntervalTimeInMillSecs;
            this.executeBizBatchSize = new ConcurrentHashMap<>();
            for (NamespaceConfig namespaceConfig : GlobalConfig.namespaceConfigs) {
                this.executeBizBatchSize.put(namespaceConfig.getNamespace(), namespaceConfig.getExecuteBizBatchSize());
            }
            redissonClient.getList(appUniqueIdentifier).add(this);
            heartbeatThread = new HeartbeatThread("TQL-Partition-Worker-Heartbeat-Thread");
            heartbeatThread.start();
            pollThread = new PollThread("TQL-Partition-Worker-Pool-Thread");
            pollThread.start();
        }else {
            log.info("worker had initialized yet!");
        }
    }

    public void shutdown() {
        final boolean initialed = initialized.get();
        if (initialed) {
            partitionSpecialBossExecutor.shutdown();
            // todo: 确定线程资源需要这样回收么？
            heartbeatThread = null;
            pollThread = null;
        }
    }

    private class HeartbeatThread extends Thread {
        public HeartbeatThread(String name) {
            super(name);
        }

        @Override
        public void run() {
            synchronized (PartitionWorker.this) {
                while (true) {
                    try {
                        final long currentTimeInMillis = System.currentTimeMillis();
                        final long sinceLastHeartTimeInMillSecs = currentTimeInMillis - lastHeartbeatTimeInMillis;
                        if (sinceLastHeartTimeInMillSecs < heartbeatIntervalTimeInMillis) {
                            final long nextHeartbeatTimeInMillis = heartbeatIntervalTimeInMillis - sinceLastHeartTimeInMillSecs;
                            wait(nextHeartbeatTimeInMillis);
                            continue;
                        }

                        final RList<PartitionWorker> remotePartitionWorkers = redissonClient.getList(appUniqueIdentifier);
                        final int size = remotePartitionWorkers.size();
                        for (int i = 0; i < size; i++) {
                            final PartitionWorker partitionWorker = remotePartitionWorkers.get(i);
                            if (partitionWorker.getWorkerUniqueIdentifier().equals(workerUniqueIdentifier)) {
                                if ((currentTimeInMillis - lastPollTimeInMillSecs) > pollIntervalTimeInMillSecs) {
                                    remotePartitionWorkers.fastRemove(i);
                                    log.info("remove this worker from remote because poll timeout And do not send heartbeat now");
                                    // todo：清除分配给此worker的 namespace和partition
                                    // heartbeat thread stop
                                    return;
                                } else {
                                    partitionWorker.setLastHeartbeatTimeInMillis(currentTimeInMillis);
                                    remotePartitionWorkers.fastSet(i, partitionWorker);
                                    updateTime();
                                }
                                return;
                            }
                        }
                        log.error("unexpected execute code here! because this worker don't have partition assigned");
                    } catch (Exception e) {
                        log.error("send heartBeat error", e);
                    }
                }
            }
        }
    }

    private class PollThread extends Thread {
        public PollThread(String name) {
            super(name);
        }

        @Override
        public void run() {
            while (true) {
                try {
                    final ConcurrentHashMap<String, List<Integer>> tempMap = new ConcurrentHashMap<>(redissonClient.getMap(appUniqueIdentifier));
                    // 当分配给此Worker的partitions数量有变化时，重新初始化boss线程池
                    if (tempMap.values().size() != latestAssignedNamespacePartitions.values().size()) {
                        latestAssignedNamespacePartitions = new ConcurrentHashMap<>(tempMap);
                        partitionSpecialBossExecutor = Executors.newFixedThreadPool(latestAssignedNamespacePartitions.values().size(), new ThreadFactory() {
                            @Override
                            public Thread newThread(Runnable r) {
                                final Thread thread = newThread(r);
                                thread.setDaemon(true);
                                // todo: 加上序号 ?
                                thread.setName("TQL-Worker-Poll-Thread-Number");
                                return thread;
                            }
                        });
                    }

                    final List<String> partitionsName = new ArrayList<>(latestAssignedNamespacePartitions.values().size());
                    latestAssignedNamespacePartitions.forEach((k, v) -> partitionsName.add(k + "_" + v));
                    for (String partitionName : partitionsName) {
                        // 当前partition的延迟队列中还有数据在poll和消费处理，则不再二次分配此partition的Boss线程资源
                        if (currentExecutingPartitionBossThread.contains(partitionName)) {
                            continue;
                        }

                        partitionSpecialBossExecutor.execute(() -> {
                            currentExecutingPartitionBossThread.add(partitionName);
                            while (true) {
                                try {
                                    // 虽然partition已经分配给此Worker了 (理论上同一个时间段内只会一个Worker在消费)，但在分布式环境下可能存在两个Worker消费一个partition的瞬时场景 (比如：新加入一个Worker引发了Master的partition reBalance导致此partition的Worker转移，则可能因为两个Worker进程间的时延而同时消费此partition)，
                                    //  所以这里再一个分布式的抢锁，做个兜底处理，以保证只会有一个Worker消费同一个partition & 新的Worker最终会获得此锁接棒继续消费此partition
                                    if (redissonClient.getLock(partitionName).tryLock(3, TimeUnit.SECONDS)) {
                                        log.info("get lock success, partition:{}, worker:{}", partitionName, workerUniqueIdentifier);
                                        break;
                                    }
                                } catch (Exception e) {
                                    log.error("try lock error, partition:" + partitionName, e);
                                }
                            }

                            final String namespace = getNamespace(partitionName);
                            final int namespaceBatchSize = executeBizBatchSize.get(namespace);
                            final List<Object> values = new ArrayList<>();
                            // 已获取到此partition的锁，此时开始poll数据消费处理
                            while (true) {
                                try {
                                    // partition的分配已经更新了的场景，在每次拉取数据处理之前，再做一下check
                                    if (!latestAssignedNamespacePartitions.containsKey(partitionName)) {
                                        break;
                                    }

                                    // 获取延迟队列数据，不从队列中删除
                                    final Object value = peekQueueValue(partitionName);
                                    // 说明在正常干活，更新心跳等时间
                                    updateTime();
                                    // 只要延迟队列中还有此partition中的数据，那么就此线程就继续干活
                                    if (value != null) {
                                        values.add(value);
                                    } else {
                                        // 如果延迟队列中此partition中已没有数据了，则退出/释放当前线程到线程池中以便复用
                                        break;
                                    }
                                } catch (Exception e) {
                                    log.error("partition thread execute error, partitionName:" + partitionName, e);
                                    break;
                                }
                                if (values.size() == namespaceBatchSize) {
                                    executeBizCallbackAndRemoveQueueValue(values, partitionName);
                                }
                            }
                            if (!values.isEmpty()) {
                                executeBizCallbackAndRemoveQueueValue(values, partitionName);
                            }
                            currentExecutingPartitionBossThread.remove(partitionName);
                        });
                    }

                    gotoSleep();
                }catch (Exception e) {
                    log.error("worker poll thread error", e);
                }
            }
        }

        private Object peekQueueValue(String partitionName) {
            return redissonClient.getBlockingDeque(partitionName).peekFirst();
        }

        private void executeBizCallbackAndRemoveQueueValue(List<Object> values, String partitionName) {
            final String namespace = getNamespace(partitionName);
            final List<CompletableFuture<Void>> cfs = new ArrayList<>();
            for (final Object value : values) {
                cfs.add(CompletableFuture.runAsync(() -> {
                    // 注：这里需要业务逻辑保障幂等性，因为在机器重启等异常情况下会重复调用
                    CallbackManager.doCallback(namespace, value);
                }).exceptionally(e -> {
                    log.error("execute biz callback code error, value:{}", value, e);
                    return null;
                }));
            }
            try {
                CompletableFuture.allOf(cfs.toArray(new CompletableFuture[0])).get(3, TimeUnit.SECONDS);
            } catch (Exception e) {
               log.error("cfu get result error, partition:{}", partitionName, e);
            } finally {
                values.forEach(v -> redissonClient.getBlockingDeque(partitionName).removeFirst());
                values.clear();
            }
        }

        private String getNamespace(String partitionName) {
            return partitionName.split("_")[0];
        }
    }

    private void updateTime() {
        final long currentTime = System.currentTimeMillis();
        this.lastPollTimeInMillSecs = currentTime;
        this.lastHeartbeatTimeInMillis = currentTime;
    }

    private void gotoSleep() {
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionWorker that = (PartitionWorker) o;
        return Objects.equals(workerUniqueIdentifier, that.workerUniqueIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(workerUniqueIdentifier);
    }
}
