package tql.delayqueue.callback;

import tql.delayqueue.utils.TQLDelayQueueException;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class CallbackManager {
    private static final ConcurrentHashMap<String, CallbackListener> consumerConcurrentHashMap = new ConcurrentHashMap<>();

    public static void registerCallback(final String namespace, final CallbackListener callbackListener) {
        if (consumerConcurrentHashMap.putIfAbsent(namespace, callbackListener) == null) {
            log.info("init, add the callback for namespace:{}", namespace);
        }else {
            log.warn("duplicate, not support multiple callback for namespace:{}, only use the first callback to execute", namespace);
        }
    }

    public static void doCallback(final String namespace, final Object data) {
        final CallbackListener callbackListener = consumerConcurrentHashMap.get(namespace);
        if (callbackListener != null) {
            try {
                callbackListener.doCallback(namespace, data);
                return;
            } catch (Exception e) {
                log.error("callback error, namespace:{}", namespace, e);
                throw new TQLDelayQueueException(e);
            }
        }
        log.error("unknown namespace for callback, namespace:{}", namespace);
    }
}
