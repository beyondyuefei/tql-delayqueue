package tql.delayqueue.partition.selector;

import tql.delayqueue.config.NamespaceConfig;

/**
 * @Description
 */
public interface PartitionSelectorService {
    int selectPartition(final String key, final NamespaceConfig namespaceConfig);
}
