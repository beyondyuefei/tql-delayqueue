package tql.delayqueue;

/**
 *
 * 提供给用户使用的延迟消息API接口，目前只支持基于Redisson的实现，在此基础上，增加了一下能力：
 *  1、保证消息的可靠性，即: 即不会因为系统重启等原因导致业务逻辑未处理完成
 *  2、支持水平扩展，通过分区partition的方式将任务分配给集群中的机器节点
 *  3、高性能，通过 namespace + partition$Index 的方式避免了高并发下的redis单key热点问题、同时回调使用多线程并发
 *
 */
public interface TQLExecuteService{
    /**
     * 提交一个延迟执行的任务
     *
     * @param delayQueueData 延迟任务的入参数据
     * @param delaySeconds 延迟执行的时间，单位：秒
     */
    <T> void executeWithFixedDelay(final DelayQueueElement<T> delayQueueData, final long delaySeconds);
}
