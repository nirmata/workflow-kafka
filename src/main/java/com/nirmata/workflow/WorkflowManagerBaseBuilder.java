package com.nirmata.workflow;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.nirmata.workflow.admin.AutoCleaner;
import com.nirmata.workflow.details.AutoCleanerHolder;
import com.nirmata.workflow.details.TaskExecutorSpec;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.QueueFactory;

import com.nirmata.workflow.serialization.Serializer;
import com.nirmata.workflow.serialization.StandardSerializer;

import org.apache.curator.framework.CuratorFramework;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;

public abstract class WorkflowManagerBaseBuilder {
    protected String instanceName;
    protected QueueFactory queueFactory;
    protected AutoCleanerHolder autoCleanerHolder = newNullHolder();
    protected Serializer serializer = new StandardSerializer();
    protected Executor taskRunnerService = MoreExecutors.newDirectExecutorService();

    protected final List<TaskExecutorSpec> specs = Lists.newArrayList();

    /**
     * <p>
     * Adds a pool of task executors for a given task type to this instance of
     * the workflow. The specified number of executors are allocated. Call this
     * method multiple times to allocate executors for the various types of tasks
     * that will be used in this workflow. You can choose to have all workflow
     * instances execute all task types or target certain task types to certain
     * instances.
     * </p>
     *
     * <p>
     * <code>qty</code> is the maximum concurrency for the given type of task for
     * this instance.
     * The logical concurrency for a given task type is the total qty of all
     * instances in the
     * workflow. e.g. if there are 3 instances in the workflow and instance A has 2
     * executors
     * for task type "a", instance B has 3 executors for task type "a" and instance
     * C has no
     * executors for task type "a", the maximum concurrency for task type "a" is 5.
     * </p>
     *
     * <p>
     * IMPORTANT: every workflow cluster must have at least one instance that has
     * task executor(s)
     * for each task type that will be submitted to the workflow. i.e workflows will
     * stall
     * if there is no executor for a given task type.
     * </p>
     *
     * @param taskExecutor the executor
     * @param qty          the number of instances for this pool
     * @param taskType     task type
     * @return this (for chaining)
     */
    public WorkflowManagerBaseBuilder addingTaskExecutor(TaskExecutor taskExecutor, int qty, TaskType taskType) {
        specs.add(new TaskExecutorSpec(taskExecutor, qty, taskType));
        return this;
    }

     /**
     * <em>optional</em><br>
     * <p>
     * Used in reporting. This will be the value recorded as tasks are executed. Via
     * reporting, you can determine which instance has executed a given task.
     * </p>
     *
     * <p>
     * Default is: <code>InetAddress.getLocalHost().getHostName()</code>
     * </p>
     *
     * @param instanceName the name of this instance
     * @return this (for chaining)
     */
    public WorkflowManagerBaseBuilder withInstanceName(String instanceName) {
        this.instanceName = Preconditions.checkNotNull(instanceName, "instanceName cannot be null");
        return this;
    }

    /**
     * Currently, only uses Kafka for queuing.
     * Send side of queue interface not implemented yet, so do not use
     *
     * @param queueFactory new queue factory
     * @return this (for chaining)
     */
    public WorkflowManagerBaseBuilder withQueueFactory(QueueFactory queueFactory) {
        this.queueFactory = Preconditions.checkNotNull(queueFactory, "queueFactory cannot be null");
        return this;
    }

    /**
     * <em>optional</em><br>
     * Sets an auto-cleaner that will run every given period. This is used to clean
     * old runs.
     * IMPORTANT: the auto cleaner will only run on the instance that is the current
     * scheduler.
     *
     * @param autoCleaner the auto cleaner to use
     * @param runPeriod   how often to run
     * @return this (for chaining)
     */
    public WorkflowManagerBaseBuilder withAutoCleaner(AutoCleaner autoCleaner, Duration runPeriod) {
        autoCleanerHolder = (autoCleaner == null) ? newNullHolder() : new AutoCleanerHolder(autoCleaner, runPeriod);
        return this;
    }

    /**
     * <em>optional</em><br>
     * By default, a JSON serializer is used to store data. Use this to
     * specify an alternate serializer
     *
     * @param serializer serializer to use
     * @return this (for chaining)
     */
    public WorkflowManagerBaseBuilder withSerializer(Serializer serializer) {
        this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");
        return this;
    }

    /**
     * <em>optional</em><br>
     * By default, tasks are run in an internal executor service. Use this to
     * specify a custom executor service for tasks. This executor does not add any
     * async/concurrency benefit. It's purpose is to allow you to control which
     * thread executes your tasks.
     *
     * @param taskRunnerService custom executor service
     * @return this (for chaining)
     */
    public WorkflowManagerBaseBuilder withTaskRunnerService(Executor taskRunnerService) {
        this.taskRunnerService = Preconditions.checkNotNull(taskRunnerService, "taskRunnerService cannot be null");
        return this;
    }

    AutoCleanerHolder newNullHolder() {
        return new AutoCleanerHolder(null, Duration.ofDays(Integer.MAX_VALUE));
    }

    public abstract WorkflowManager build();

    public abstract WorkflowManagerBuilder withCurator(CuratorFramework curator, String namespace, String version);

    public abstract WorkflowManagerKafkaBuilder withoutWorkflowWorker();

    public abstract WorkflowManagerKafkaBuilder withMongo(String connStr);

    public abstract WorkflowManagerKafkaBuilder withKafka(String brokers, String namespace, String version, int taskTypeParitions, short replicas);

    public abstract WorkflowManagerKafkaBuilder withKafka(String brokers, String namespace, String version);
}
