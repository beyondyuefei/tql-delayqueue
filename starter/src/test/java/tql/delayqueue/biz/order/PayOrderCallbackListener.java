package tql.delayqueue.biz.order;

import lombok.extern.slf4j.Slf4j;
import tql.delayqueue.Namespace;
import tql.delayqueue.callback.CallbackListener;
@Namespace(name = "payOrder", executeBatchSize = 2)
@Slf4j
public class PayOrderCallbackListener implements CallbackListener {
    @Override
    public <T> void doCallback(String namespace, T data) {
        log.info("payOrder callback, namespace:"+ namespace + ", data:" + data);
    }
}
