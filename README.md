# tql-delayqueue

#### 介绍
  - 这是一个延迟消息框架，延迟消息支持秒级精度、消息高可靠性、水平扩展，底层基于redisson实现
  - 写这个的原因是在公司的电商项目中，发现有类似的场景都需要这种能力(如：订单未支付超时后的回调通知以变更订单状态逻辑、多少时间后发起外卖骑手的呼叫等等)：
      - 如果是直接使用redisson的延迟队列，则会存在消息丢失的可靠性缺陷问题、高并发发送延迟消息中可能存在redis单key热点、使用的模版代码繁琐不通用等
      - 调研发现一般大公司会基于消息中间件(如: kafka、Rabbitmq等)做定制化开发，但是这不具备通用性且大多只支持时间段级别的延迟 (1-3min一个时间段、4-10min一个时间段等等)即不支持秒级的延迟精度
      - 同时，对于很多中小企业来说，更多的是业务发展，而非去实现一个延迟消息框架
      - 底层基于redisson而非消息中间件，是因为redis目前是几乎所有项目都会使用的、轻量的，而消息中间件则不一定、偏重的
  
        
#### 阶段 
  ##### 目前完成了功能最小集的开发:
  - 支持namespace级别的延迟消息发送与隔离
  - 支持基于partition的消息分区以及loadbalance
  - 支持 one partition one poll queue thread 的线程封闭拉取队列消息和删除，同时支持在回调业务时可多线程并发执行 
  
  ##### 接下来
  - 会逐步扩大单元测试的覆盖面以check核心功能的可靠
  - 也会尽快完善相关的设计和使用文档
  - 会增加一些扩展点以支持AOP能力、数据指标能力等等

#### 快速开始
1、引入maven依赖 (以常见的spring starter为例)

```
   <dependency>
      <groupId>tql</groupId>
      <artifactId>delayqueue-spring-boot-starter</artifactId>
      <version>1.0-SNAPSHOT</version>
    </dependency>
```

2、在你的Spring服务中注入tql服务并发送延迟消息

```
@Autowired
private TQLExecuteService tqlExecuteService;

public void sendDelayOrderCallbackMessage() {
   tqlExecuteService.executeWithFixedDelay(new DelayQueueElement<>(orderCode), orderDataMap, "payOrder"), 60);
}
```

3、实现namespace级别的回调逻辑

```
@Namespace(name = "payOrder")
@Slf4j
public class PayOrderCallbackListener implements CallbackListener {
    @Override
    public <T> void doCallback(String namespace, T data) {
        log.info("payOrder callback, namespace:"+ namespace + ", data:" + data);
    }
}

4、环境要求
- base jdk8+
- redis
```

#### 基础概念

todo~


#### 软件架构

todo~

