package com.nirmata.workflow;

import org.apache.curator.framework.CuratorFramework;

public class WorkflowManagerUtil {
    private WorkflowManagerUtil() {
    }

    public static WorkflowManagerBaseBuilder createBuilder(CuratorFramework curator, String kafkaAddress,
            String namespace, String version, boolean useCuratorForWorkflow) {
        if (useCuratorForWorkflow) {
            return WorkflowManagerBuilder.builder().withCurator(curator, namespace, version);
        }
        return WorkflowManagerKafkaBuilder.builder().withKafka(kafkaAddress, namespace, version);
    }
}
