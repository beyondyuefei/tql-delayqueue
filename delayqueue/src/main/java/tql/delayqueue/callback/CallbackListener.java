package tql.delayqueue.callback;

/**
 * 使用方在初始化阶段将对应 namespace的执行逻辑注册进来，后续 (定时轮询队列的任务) 在触发 redisson 延迟队列回调时会执行 doCallback 方法
 *
 *
 @Description
 */
@FunctionalInterface
public interface CallbackListener {
    <T> void doCallback(final String namespace, final T data);
}
