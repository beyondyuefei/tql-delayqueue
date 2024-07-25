package tql.delayqueue.autoconfigure;

import tql.delayqueue.TQLExecuteService;
import tql.delayqueue.TQLExecuteServiceFactory;
import tql.delayqueue.callback.CallbackListener;
import tql.delayqueue.callback.CallbackManager;
import tql.delayqueue.config.GlobalConfig;
import tql.delayqueue.config.NamespaceConfig;
import tql.delayqueue.partition.PartitionMaster;
import tql.delayqueue.partition.PartitionWorker;
import tql.delayqueue.utils.TQLDelayQueueException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * @Description
 */
@Configuration
@EnableConfigurationProperties(TQLDelayQueueProperties.class)
@Slf4j
public class TQLDelayQueueConfiguration implements InitializingBean, DisposableBean {
    @Autowired
    private TQLDelayQueueProperties tqlDelayQueueProperties;
    private PartitionMaster partitionMaster;
    private PartitionWorker partitionWorker;
    private final ApplicationContext applicationContext;

    public TQLDelayQueueConfiguration(final ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public void afterPropertiesSet() {
        final Map<String, CallbackListener> listenerMap = applicationContext.getBeansOfType(CallbackListener.class);
        if(listenerMap.isEmpty()) {
            log.warn("not found TQL-DelayQueue CallbackListener impl, so do not start TQL-DelayQueue and return now!");
            return;
        }

        if (tqlDelayQueueProperties.getAppUniqueIdentifier() != null) {
            GlobalConfig.appUniqueIdentifier = tqlDelayQueueProperties.getAppUniqueIdentifier();
        }

        if (tqlDelayQueueProperties.getNoHeartBeatTimeInMillSec() != null) {
            GlobalConfig.noHeartBeatTimeInMillSec = tqlDelayQueueProperties.getNoHeartBeatTimeInMillSec();
        }

        if (tqlDelayQueueProperties.getHeartbeatIntervalTimeInMillis() != null) {
            GlobalConfig.heartbeatIntervalTimeInMillis = tqlDelayQueueProperties.getHeartbeatIntervalTimeInMillis();
        }

        if (tqlDelayQueueProperties.getPollIntervalTimeInMillSecs() != null) {
            GlobalConfig.pollIntervalTimeInMillSecs = tqlDelayQueueProperties.getPollIntervalTimeInMillSecs();
        }

        if (tqlDelayQueueProperties.getRedisHost() != null) {
            GlobalConfig.redisHost = tqlDelayQueueProperties.getRedisHost();
        }

        if (tqlDelayQueueProperties.getRedisPort() != null) {
            GlobalConfig.redisPort = tqlDelayQueueProperties.getRedisPort();
        }

        if (tqlDelayQueueProperties.getRedisPassword() != null) {
            GlobalConfig.redisPassword = tqlDelayQueueProperties.getRedisPassword();
        }

        if (tqlDelayQueueProperties.getRedisDatabase() != null) {
            GlobalConfig.redisDatabase = tqlDelayQueueProperties.getRedisDatabase();
        }

        for (final Map.Entry<String, CallbackListener> callbackListenerEntry : listenerMap.entrySet()) {
            final String beanName = callbackListenerEntry.getKey();
            final CallbackListener callbackListener = callbackListenerEntry.getValue();
            final Namespace namespace = callbackListener.getClass().getAnnotation(Namespace.class);
            if (namespace == null) {
                final String errorMsg = "can not find @Namespace on bean:" + beanName;
                log.error(errorMsg);
                throw new TQLDelayQueueException(errorMsg);
            }
            GlobalConfig.namespaceConfigs.add(new NamespaceConfig(namespace.name(), namespace.partitionSize(), namespace.executeBatchSize()));
            CallbackManager.registerCallback(namespace.name(), callbackListener);
        }

        partitionMaster = new PartitionMaster();
        partitionMaster.init();
        partitionWorker = new PartitionWorker();
        partitionWorker.init();
    }

    @Override
    public void destroy() {
        partitionMaster.shutdown();
        partitionWorker.shutdown();
    }

    @Bean
    public TQLExecuteService tqlExecuteService() {
        return TQLExecuteServiceFactory.get();
    }
}
