package tql.delayqueue.biz.order

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import tql.delayqueue.Namespace
import tql.delayqueue.callback.CallbackListener

@Namespace(name = "payOrder", executeBatchSize = 2)
class PayOrderCallbackListener:CallbackListener {
    private val logger: Logger = LoggerFactory.getLogger(PayOrderCallbackListener::class.java)

    override fun <T : Any?> doCallback(namespace: String?, data: T) {
        logger.info("kt: payOrder callback, namespace:$namespace, data:$data")
    }
}