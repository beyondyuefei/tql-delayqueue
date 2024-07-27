package tql.delayqueue.partition;

import tql.delayqueue.config.GlobalConfig;
import tql.delayqueue.config.NamespaceConfig;
import tql.delayqueue.partition.assigner.PartitionAssignerFactory;
import tql.delayqueue.partition.assigner.PartitionAssignerService;
import tql.delayqueue.redisson.RedissonClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RList;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 1. RMap<String, List<Worker> , key: appUniqueCode、 value: {"ip":"1.1.1.1","status":"on/off"}
 * master 维护 Worker List (heartbeat)、分配&绑定partitions与Worker的关系
 * <p>
 * 2. RMap<String, RMap<String, List<Integer>>>, key: WorkerUniqueCode (master自身也是一个Worker) 、 value : key 是 namespace、value 是 partitions
 * Worker消费这个数据结构，获取Master分配给自己的partition
 * <p>
 * 3. 当Worker正常(执行shutdown) 或 直接 kill -9 崩溃，会触发执行一个基于lua脚本的原子的操作: 删除 1 和 2 中对应 Worker的信息
 *
 *
 * todo 待优化性能: 对 Redisson API中的数据结构，如: RMap、RList 的每一次操作几乎都会涉及到远程与Redis的交互，如果大量 Worker (成白上千的数量) 会是一个迟延问题
 *
 */
@Slf4j
public class PartitionMaster {
    private final RedissonClient redissonClient = RedissonClientFactory.getInstance();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            final Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("TQL-DelayQueue-PartitionMaster-Thread");
            return thread;
        }
    });
    private String appUniqueIdentifier;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private volatile boolean masterOwner;

    private volatile List<PartitionWorker> localPartitionWorkers = Collections.emptyList();
    /**
     * 无心跳时间的最大边界值，超出将认为失联
     */
    private long noHeartBeatTimeInMillSec;
    private RLock redissonClientLock;
    private PartitionAssignerService partitionAssignerService;

    public void init() {
        if (initialized.compareAndSet(false, true)) {
            this.appUniqueIdentifier = GlobalConfig.appUniqueIdentifier;
            final String lockName = appUniqueIdentifier + "_" + "master";
            redissonClientLock = redissonClient.getLock(lockName);
            partitionAssignerService = PartitionAssignerFactory.create(GlobalConfig.partitionAssignerName);
            noHeartBeatTimeInMillSec = GlobalConfig.noHeartBeatTimeInMillSec;

            executorService.execute(() -> {
                while (true) {
                    try {
                        final boolean lockSuccess = redissonClientLock.tryLock(3, TimeUnit.SECONDS);
                        if (lockSuccess) {
                            if (!masterOwner) {
                                masterOwner = true;
                                log.info("become master owner!");
                            }
                            reBalanceWorkersPartition();
                        } else {
                            masterOwner = false;
                        }
                        TimeUnit.SECONDS.sleep(3);
                    } catch (Exception e) {
                        log.error("master occur error", e);
                    }
                }
            });
        } else {
            log.info("master had initialized yet!");
        }
    }

    public void shutdown() {
        final boolean initialed = initialized.get();
        if (initialed) {
            executorService.shutdown();
            if (masterOwner) {
                redissonClientLock.unlock();
            }
        }
    }


    private void reBalanceWorkersPartition() {
        if (!masterOwner) {
            log.warn("unexpected here, mastOwner:{}", masterOwner);
            return;
        }

        final RList<PartitionWorker> remotePartitionWorkers = redissonClient.getList(appUniqueIdentifier);
        final int remotePartitionWorkersSize = remotePartitionWorkers.size();
        final int localPartitionWorkersSize = localPartitionWorkers.size();
        if ((remotePartitionWorkersSize != localPartitionWorkersSize)
                // 防止: 当原master owner 网络断开、另外一个master接管且Workers身份也出现变化、但数量一样，之后本master又成为owner时的场景
                || !remotePartitionWorkers.equals(localPartitionWorkers)) {
            log.info("trigger reBalance Worker Partition, because local workers different with remote. localPartitionWorkers:{}, remotePartitionWorkers:{}", localPartitionWorkers, remotePartitionWorkers);
            localPartitionWorkers = new ArrayList<>(remotePartitionWorkers);
            reBalanceWorkersPartition0(localPartitionWorkers);
            return;
        }

        final List<Integer> unHealthWorkersIndex = new ArrayList<>();
        for (int i = 0; i < remotePartitionWorkersSize; i++) {
            if (isWorkerNoHeartBeat(remotePartitionWorkers.get(i))) {
                unHealthWorkersIndex.add(i);
            }
        }

        if (!unHealthWorkersIndex.isEmpty()) {
            unHealthWorkersIndex.forEach(remotePartitionWorkers::fastRemove);
            localPartitionWorkers = new ArrayList<>(remotePartitionWorkers);
            reBalanceWorkersPartition0(localPartitionWorkers);
        }
    }

    private void reBalanceWorkersPartition0(final List<PartitionWorker> partitionWorkers) {
        if (partitionWorkers.isEmpty()) {
            log.info("partitionWorkers is empty");
            return;
        }

        final List<NamespaceConfig> namespaceConfigs = GlobalConfig.namespaceConfigs;
        for (NamespaceConfig namespaceConfig : namespaceConfigs) {
            final Map<String, List<Integer>> workersPartition = partitionAssignerService.assignWorkersPartition(namespaceConfig, partitionWorkers);
            log.info("reBalance workers partition, namespace:{}, partitionInfo:{}", namespaceConfig.getNamespace(), workersPartition);
            // set to redisson, todo: 可以优化 ? 提取 getMap，使得远程不需要每次都远程 getMap ?
            redissonClient.getMap(appUniqueIdentifier + "_" + namespaceConfig.getNamespace()).putAll(workersPartition);
        }
    }

    private boolean isWorkerNoHeartBeat(PartitionWorker partitionWorker) {
        final long lastHeartbeatTimeInMillis = partitionWorker.getLastHeartbeatTimeInMillis();
        final long timeGapInMillSec = System.currentTimeMillis() - lastHeartbeatTimeInMillis;
        if (timeGapInMillSec > noHeartBeatTimeInMillSec) {
            log.warn("the worker: {} no heartBeat over:{}, crash or reStarting ? lastHeartbeatTimeInMillis:{}, timeGap:{}", partitionWorker.getWorkerUniqueIdentifier(), noHeartBeatTimeInMillSec, lastHeartbeatTimeInMillis, timeGapInMillSec);
            return true;
        } else {
            return false;
        }
    }
}
