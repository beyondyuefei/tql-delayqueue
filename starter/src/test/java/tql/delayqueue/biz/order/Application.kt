package tql.delayqueue.biz.order

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.ComponentScan

@SpringBootApplication
@ComponentScan(basePackages = ["tql.delayqueue"])
open class Application {
    private val logger:Logger = LoggerFactory.getLogger(Application::class.java)

    fun main() {
        try {
            SpringApplication.run(Application::class.java)
        }catch (e:Exception) {
            logger.error("app run error", e)
            throw RuntimeException(e)
        }
    }
}