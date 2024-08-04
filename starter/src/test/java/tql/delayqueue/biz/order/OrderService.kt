package tql.delayqueue.biz.order;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tql.delayqueue.DelayQueueElement;
import tql.delayqueue.TQLExecuteService;

/**
 * @Description
 */
@Service
@Slf4j
public class OrderService {
    @Autowired
    private TQLExecuteService tqlExecuteService;

    public void init() {
        log.info("order service init start");
        for (int i = 1;i < 10;i++) {
            tqlExecuteService.executeWithFixedDelay(new DelayQueueElement<>(String.valueOf(i), "order" + "_" + i, "payOrder"), 1);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignore
            }
        }
       log.info("order service init success");
    }
}
