package tql.delayqueue.partition.assigner.impl

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import tql.delayqueue.config.NamespaceConfig
import tql.delayqueue.partition.PartitionWorker

class RoundRobinPartitionAssignerServiceImplTest {
    private val roundRobinPartitionAssignerServiceImpl = RoundRobinPartitionAssignerServiceImpl()

    @Test
    fun testSameWeight() {
        val worker1 = PartitionWorker(10, "worker1")
        val worker2 = PartitionWorker(10, "worker2")
        val worker3 = PartitionWorker(10, "worker3")
        val workers = listOf(worker1, worker2, worker3)
        val namespaceConfig = NamespaceConfig("testSameWeight", 9, 1)
        val workersPartitionMap = roundRobinPartitionAssignerServiceImpl.assignWorkersPartition(namespaceConfig, workers)
        Assertions.assertTrue(workersPartitionMap["worker1"]!!.size == 3)
        Assertions.assertTrue(workersPartitionMap["worker2"]!!.size == 3)
        Assertions.assertTrue(workersPartitionMap["worker3"]!!.size == 3)
    }


    @Test
    fun testDiffWeight() {
        val worker1 = PartitionWorker(100, "worker1")
        val worker2 = PartitionWorker(50, "worker2")
        val worker3 = PartitionWorker(25, "worker3")
        val workers = listOf(worker1, worker2, worker3)
        val namespaceConfig = NamespaceConfig("testDiffWeight", 30, 1)
        val workersPartitionMap = roundRobinPartitionAssignerServiceImpl.assignWorkersPartition(namespaceConfig, workers)
        val work1PartitionSize = workersPartitionMap["worker1"]!!.size
        val work2PartitionSize = workersPartitionMap["worker2"]!!.size
        val work3PartitionSize = workersPartitionMap["worker3"]!!.size
        println("work1PartitionSize:$work1PartitionSize, work2PartitionSize:$work2PartitionSize, work3PartitionSize:$work3PartitionSize")
        Assertions.assertTrue { work1PartitionSize > work2PartitionSize && work1PartitionSize > work3PartitionSize && work2PartitionSize > work3PartitionSize}
    }
}