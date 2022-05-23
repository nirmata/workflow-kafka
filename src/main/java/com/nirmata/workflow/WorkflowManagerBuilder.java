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

import com.google.common.base.Preconditions;
import com.nirmata.workflow.details.WorkflowManagerImpl;
import com.nirmata.workflow.queue.zookeeper.ZooKeeperSimpleQueueFactory;

import org.apache.curator.framework.CuratorFramework;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Builds {@link WorkflowManager} instances
 */
public class WorkflowManagerBuilder extends WorkflowManagerBaseBuilder
{
    private CuratorFramework curator;


    /**
     * Return a new builder
     *
     * @return new builder
     */
    public static WorkflowManagerBuilder builder()
    {
        return new WorkflowManagerBuilder();
    }

    /**
     * <strong>required</strong><br>
     * Set the Curator instance to use. In addition
     * to the Curator instance, specify a namespace for the workflow and a version. The namespace
     * and version combine to create a unique workflow. All instances using the same namespace and version
     * are logically part of the same workflow.
     *
     * @param curator Curator instance
     * @param namespace workflow namespace
     * @param version workflow version
     * @return this (for chaining)
     */
    public WorkflowManagerBuilder withCurator(CuratorFramework curator, String namespace, String version)
    {
        curator = Preconditions.checkNotNull(curator, "curator cannot be null");
        namespace = Preconditions.checkNotNull(namespace, "namespace cannot be null");
        version = Preconditions.checkNotNull(version, "version cannot be null");
        this.curator = curator.usingNamespace(namespace + "-" + version);
        return this;
    }

    @Override
    public WorkflowManagerKafkaBuilder withoutWorkflowWorker() {
        throw new UnsupportedOperationException("Building Wflow without Wflow Worker is unsupported for Curator based Wflow");
    }

    @Override
    public WorkflowManagerKafkaBuilder withMongo(String connStr) {
        throw new UnsupportedOperationException("Building Wflow with Mongo is unsupported for Curator based Wflow");
    }

    @Override
    public WorkflowManagerKafkaBuilder withKafka(String brokers, String namespace, String version, int taskTypeParitions, short replicas) {
        throw new UnsupportedOperationException("Building Wflow with Kafka is unsupported for Curator based Wflow");
    }

    @Override
    public WorkflowManagerKafkaBuilder withKafka(String brokers, String namespace, String version) {
        throw new UnsupportedOperationException("Building Wflow with Kafka is unsupported for Curator based Wflow");
    }

    /**
     * Return a new WorkflowManager using the current builder values
     *
     * @return new WorkflowManager
     */
    public WorkflowManager build()
    {
        return new WorkflowManagerImpl(curator, super.queueFactory, super.instanceName, super.specs, super.autoCleanerHolder, super.serializer, super.taskRunnerService);
    }

    private WorkflowManagerBuilder()
    {
        super.queueFactory = new ZooKeeperSimpleQueueFactory();
        try
        {
            super.instanceName = InetAddress.getLocalHost().getHostName();
        }
        catch ( UnknownHostException e )
        {
            super.instanceName = "unknown";
        }
    }
}
