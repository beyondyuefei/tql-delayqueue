package tql.delayqueue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import tql.delayqueue.biz.order.Application;
import tql.delayqueue.config.GlobalConfig;

@SpringBootTest(classes = Application.class)
public class StarterTest {
    @Autowired
    private TQLExecuteService tqlExecuteService;

    @Test
    public void test() {
        Assertions.assertNotNull(tqlExecuteService);
        Assertions.assertFalse(GlobalConfig.namespaceConfigs.isEmpty());
    }
}
