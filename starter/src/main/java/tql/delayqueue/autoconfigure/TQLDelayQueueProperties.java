package tql.delayqueue.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @Description
 */
@Data
@ConfigurationProperties(prefix = "tql-delay-queue")
public class TQLDelayQueueProperties {
    private String appUniqueIdentifier;
    private Integer noHeartBeatTimeInMillSec;
    private Integer heartbeatIntervalTimeInMillis;
    private Integer pollIntervalTimeInMillSecs;
    private String partitionAssignerName;
    private String partitionSelectorName;
    private String redisHost;
    private Integer redisPort;
    private String redisPassword;
    private Integer redisDatabase;

}
