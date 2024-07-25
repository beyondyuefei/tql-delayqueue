package tql.delayqueue.autoconfigure;

import org.springframework.stereotype.Component;
import tql.delayqueue.config.NamespaceConfig;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@link NamespaceConfig}
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Component
public @interface Namespace {
    String name();
    int partitionSize() default 1;
    int executeBatchSize() default 1;
}
