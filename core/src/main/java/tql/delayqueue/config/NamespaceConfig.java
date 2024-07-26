package tql.delayqueue.config;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 @Description
 */
@Getter
public class NamespaceConfig {
    private final String namespace;
    private final int partitionSize;
    /**
     * 一次性批量处理的消息数量，默认为 1 (即：按照入队列的顺序，有序的执行回调)
     */
    private final int executeBizBatchSize;

    public NamespaceConfig(String namespace, int partitionSize, int executeBizBatchSize) {
        if (StringUtils.isBlank(namespace)) {
            throw new IllegalArgumentException("namespace can not be blank!");
        }
        this.namespace = namespace;
        this.partitionSize = Math.max(partitionSize, 1);
        this.executeBizBatchSize = Math.max(executeBizBatchSize, 1);
    }
}
