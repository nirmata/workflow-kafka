package com.nirmata.workflow;

import org.apache.curator.framework.CuratorFramework;

public class WorkflowManagerUtil {
    private WorkflowManagerUtil() {
    }

    /**
     * Create and return a Workflow variant based on the type of Workflow used by the client
     * @see WorkflowManagerKafkaBuilder
     * @see WorkflowManagerBuilder
     *
     * @param curator CuratorFramework
     * @param kafkaAddress kafka broker address string
     * @param namespace workflow namespace
     * @param version workflow version
     * @param useCuratorForWorkflow whether client uses curator for workflow. This value can be set using an
     * environment variable which allows easy switching across the workflow types
     *
     * @return WorkflowManagerBaseBuilder (for chaining) returns a builder withCurator() if curator is being used
     * by the client else returns a builder withKafka()
     */

    public static WorkflowManagerBaseBuilder createBuilder(CuratorFramework curator, String kafkaAddress,
            String namespace, String version, boolean useCuratorForWorkflow) {
        if (useCuratorForWorkflow) {
            return WorkflowManagerBuilder.builder().withCurator(curator, namespace, version);
        }
        return WorkflowManagerKafkaBuilder.builder().withKafka(kafkaAddress, namespace, version);
    }
}
