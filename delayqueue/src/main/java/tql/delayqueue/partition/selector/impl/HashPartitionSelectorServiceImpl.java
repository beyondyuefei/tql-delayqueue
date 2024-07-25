package tql.delayqueue.partition.selector.impl;

import tql.delayqueue.config.NamespaceConfig;
import tql.delayqueue.partition.selector.PartitionSelectorService;
import tql.delayqueue.utils.Utils;

import java.nio.charset.StandardCharsets;

/**
 * 默认的 key -> partition 的实现，采用 hash 映射
 *
 * @Description
 */
public class HashPartitionSelectorServiceImpl implements PartitionSelectorService {
    @Override
    public int selectPartition(String key, NamespaceConfig namespaceConfig) {
        return Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8)))
                % namespaceConfig.getPartitionSize();
    }
}