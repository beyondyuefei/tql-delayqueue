# tql-delayqueue

#### 介绍
延迟消息框架，延迟消息支持秒级精度、消息高可靠性、水平扩展，基于redisson实现

#### 快速开始
1、引入maven依赖 (以常见的spring starter为例)

```
   <dependency>
      <groupId>tql</groupId>
      <artifactId>delayqueue-spring-boot-starter</artifactId>
      <version>1.0-SNAPSHOT</version>
    </dependency>
```

2、在你的Spring服务中注入API服务

```
@Autowired
private TQLExecuteService tqlExecuteService;
```

3、实现namespace级别的回调逻辑

```
@Namespace(name = "payOrder", executeBatchSize = 2)
@Slf4j
public class PayOrderCallbackListener implements CallbackListener {
    @Override
    public <T> void doCallback(String namespace, T data) {
        log.info("payOrder callback, namespace:"+ namespace + ", data:" + data);
    }
}
```

#### 基础概念

todo~


#### 软件架构

todo~

