/**
 * Copyright 2014 Nirmata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
