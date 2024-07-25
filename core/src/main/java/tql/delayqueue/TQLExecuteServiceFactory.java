package tql.delayqueue;

/**
 * 用户使用的API入口，可以通过： final TQLExecuteService tqlExecuteService = TQLExecuteServiceFactory.get() 的方式使用
 */
public class TQLExecuteServiceFactory {
    private static TQLExecuteService tqlExecuteService;
    public synchronized static TQLExecuteService get() {
        if (tqlExecuteService == null) {
            tqlExecuteService = new RedissonTQLExecuteServiceImpl();
        }
        return tqlExecuteService;
    }
}
