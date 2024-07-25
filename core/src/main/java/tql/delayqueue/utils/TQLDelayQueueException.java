package tql.delayqueue.utils;

public class TQLDelayQueueException  extends RuntimeException{
    public TQLDelayQueueException() {
        super();
    }

    public TQLDelayQueueException(String message) {
        super(message);
    }

    public TQLDelayQueueException(String message, Throwable cause) {
        super(message, cause);
    }

    public TQLDelayQueueException(Throwable cause) {
        super(cause);
    }
}
