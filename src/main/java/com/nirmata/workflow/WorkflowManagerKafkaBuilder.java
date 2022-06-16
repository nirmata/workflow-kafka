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

import com.nirmata.workflow.details.KafkaHelper;
import com.nirmata.workflow.details.WorkflowManagerKafkaImpl;
import com.nirmata.workflow.queue.kafka.KafkaSimpleQueueFactory;
import com.nirmata.workflow.storage.StorageManager;
import com.nirmata.workflow.storage.StorageManagerMongoImpl;
import com.nirmata.workflow.storage.StorageManagerNoOpImpl;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Builds {@link WorkflowManager} instances
 */
public class WorkflowManagerKafkaBuilder extends WorkflowManagerBaseBuilder {
    private KafkaHelper kafkaHelper;
    private boolean workflowWorkerEnabled = true;
    private StorageManager storageManager = new StorageManagerNoOpImpl();

    private String namespace = "";
    private String namespaceVer = "";

    /**
     * Return a new builder
     *
     * @return new builder
     */
    public static WorkflowManagerKafkaBuilder builder() {
        return new WorkflowManagerKafkaBuilder();
    }

    /**
     * <strong>required</strong><br>
     * Set the Kafka broker to use. In addition
     * to it, specify a namespace for the workflow and a version.
     * The namespace
     * and version combine to create a unique workflow. All instances using the same
     * namespace and version
     * are logically part of the same workflow.
     *
     * @param kafkaServer Kafka bootstrap servers (e.g. localhost:9092,xxx:1010)
     * @param namespace   workflow namespace
     * @param version     workflow version
     * @return this (for chaining)
     */
    public WorkflowManagerKafkaBuilder withKafka(String brokers, String namespace, String version) {
        return this.withKafka(brokers, namespace, version, 10, (short) 1);
    }

    /**
     * <strong>required</strong><br>
     * Set the Kafka broker to use. In addition
     * to it, specify a namespace for the workflow and a version.
     * The namespace
     * and version combine to create a unique workflow. All instances using the same
     * namespace and version
     * are logically part of the same workflow.
     *
     * @param kafkaServer        Kafka bootstrap servers (e.g.
     *                           localhost:9092,xxx:1010)
     * @param namespace          workflow namespace
     * @param version            workflow version
     * @param taskTypePartitions partitions for tasktype topics, at least equal to
     *                           max replicas of services with executors for a task
     *                           type
     * @param replicas           number of replicas for topics
     * @return this (for chaining)
     */
    public WorkflowManagerKafkaBuilder withKafka(String brokers, String namespace, String version,
            int taskTypeParitions, short replicas) {
        this.kafkaHelper = new KafkaHelper(brokers, namespace, version, 1, taskTypeParitions, replicas);
        this.namespace = kafkaHelper.getNamespace();
        this.namespaceVer = kafkaHelper.getVersion();

        return this;
    }

    /**
     * Set the MongoDB client to use. In addition
     * to it, specify a namespace for the workflow and a version.
     * The namespace and version combine to create a unique workflow.
     * All instances using the same namespace and version
     * are logically part of the same workflow and their data is stored in
     * the same collection. If not specified, data is not stored in Mongo
     * and admin commands also give empty response
     *
     * @param mongoUri  MongoDB connection string
     * @param namespace workflow namespace
     * @param version   workflow version
     * @return this (for chaining)
     */
    public WorkflowManagerKafkaBuilder withMongo(String connStr) {
        this.storageManager = new StorageManagerMongoImpl(connStr, namespace, namespaceVer);
        return this;
    }

    /**
     * Don't enable the workflow worker, typically used when you want to use
     * workflow manager to submit tasks only, not to execute workflow DAGs
     *
     * @return this (for chaining)
     */
    public WorkflowManagerKafkaBuilder withoutWorkflowWorker() {
        this.workflowWorkerEnabled = false;

        return this;
    }

    /**
     * Return a new WorkflowManager using the current builder values
     *
     * @return new WorkflowManager
     */
    public WorkflowManager build() {
        return new WorkflowManagerKafkaImpl(kafkaHelper, storageManager, workflowWorkerEnabled, queueFactory,
                instanceName, specs, autoCleanerHolder, serializer, taskRunnerService);
    }

    private WorkflowManagerKafkaBuilder() {
        queueFactory = new KafkaSimpleQueueFactory();
        try {
            instanceName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            instanceName = "unknown";
        }
        this.kafkaHelper = new KafkaHelper("localhost:9092", "defaultns", "v1");
    }
}
