package tql.delayqueue.biz.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;

/**
 * @Description
 */
@SpringBootApplication
@ComponentScan(basePackages = {"tql.delayqueue"})
public class Application {
    private static ConfigurableApplicationContext context;

    public static void main(String[] args) {
        try {
            Application.context = SpringApplication.run(Application.class, args);
            System.out.println("=======================启动成功==============================");
            OrderService orderService = context.getBean(OrderService.class);
            orderService.init();
        } catch (Throwable t) {
            t.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

}
