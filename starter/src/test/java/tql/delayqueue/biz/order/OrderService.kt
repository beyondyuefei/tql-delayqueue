package tql.delayqueue.biz.order

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import tql.delayqueue.DelayQueueElement
import tql.delayqueue.TQLExecuteService
import javax.annotation.PostConstruct

@Service
class OrderService {
    @Autowired
    private lateinit var tqlExecuteService: TQLExecuteService
    private val logger: Logger = LoggerFactory.getLogger(OrderService::class.java)

    @PostConstruct
    fun init() {
        logger.info("kt: order service init start")
        for (i in 1..10) {
            tqlExecuteService.executeWithFixedDelay(DelayQueueElement(i.toString(), "order_$i", "payOrder"), 1)
            try {
              Thread.sleep(1000)
            } catch (e:InterruptedException) {
                // ignore
            }
        }
        logger.info("order service init success")
    }
}