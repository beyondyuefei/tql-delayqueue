package tql.delayqueue;

import lombok.Getter;

/**
 * 延迟消息队列中的元素
 */
@Getter
public class DelayQueueElement<T> {
    /**
     * 主要用于数据分区使用的主键
     */
    private final String key;
    /**
     * 业务数据
     */
    private final T data;

    /**
     * namespace
     */
    private final String namespace;

    public DelayQueueElement(String key, T data, String namespace) {
        this.key = key;
        this.data = data;
        this.namespace = namespace;
    }
}
