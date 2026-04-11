/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.RemovedTaskListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNullElse;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;

public class TransportListTasksAction extends TransportTasksAction<Task, ListTasksRequest, ListTasksResponse, TaskInfo> {

    public static final ActionType<ListTasksResponse> TYPE = new ActionType<>("cluster:monitor/tasks/lists");

    public static long waitForCompletionTimeout(TimeValue timeout) {
        if (timeout == null) {
            timeout = DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT;
        }
        return System.nanoTime() + timeout.nanos();
    }

    private static final TimeValue DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT = timeValueSeconds(30);

    @Inject
    public TransportListTasksAction(ClusterService clusterService, TransportService transportService, ActionFilters actionFilters) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            ListTasksRequest::new,
            TaskInfo::from,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
        );
    }

    @Override
    protected ListTasksResponse newResponse(
        ListTasksRequest request,
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        return new ListTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected void taskOperation(CancellableTask actionTask, ListTasksRequest request, Task task, ActionListener<TaskInfo> listener) {
        TaskInfo info = task.taskInfo(clusterService.localNode().getId(), request.getDetailed());
        if (task instanceof BulkByScrollTask bbs) {
            info = bbs.rewriteTaskInfoForRelocation(info);
        }
        listener.onResponse(info);
    }

    @Override
    protected void doExecute(Task task, ListTasksRequest request, ActionListener<ListTasksResponse> listener) {
        assert task instanceof CancellableTask;
        if (couldMatchReindexAction(request) == false) {
            super.doExecute(task, request, listener);
            return;
        }
        TimeValue originalTimeout = request.getTimeout();
        long deadline = System.nanoTime() + requireNonNullElse(originalTimeout, DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT).nanos();
        request.setTimeout(TimeValue.timeValueNanos(requireNonNullElse(originalTimeout, DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT).nanos() / 2));
        super.doExecute(task, request, listener.delegateFailureAndWrap((first, firstResponse) -> {
            long remaining = Math.max(deadline - System.nanoTime(), 0);
            request.setTimeout(TimeValue.timeValueNanos(remaining));
            super.doExecute(task, request, first.delegateFailureAndWrap((second, secondResponse) -> {
                List<TaskInfo> combined = new ArrayList<>(secondResponse.getTasks().size() + firstResponse.getTasks().size());
                combined.addAll(secondResponse.getTasks());
                combined.addAll(firstResponse.getTasks());
                second.onResponse(
                    new ListTasksResponse(deduplicateTasks(combined), secondResponse.getTaskFailures(), secondResponse.getNodeFailures())
                );
            }));
        }));
    }

    private static boolean couldMatchReindexAction(ListTasksRequest request) {
        String[] actions = request.getActions();
        if (CollectionUtils.isEmpty(actions)) {
            return true;
        }
        return Regex.simpleMatch(actions, ReindexAction.NAME);
    }

    static List<TaskInfo> deduplicateTasks(List<TaskInfo> tasks) {
        if (tasks.size() <= 1) {
            return tasks;
        }
        Map<TaskId, TaskInfo> seen = new LinkedHashMap<>(tasks.size());
        for (TaskInfo task : tasks) {
            seen.putIfAbsent(task.taskId(), task);
        }
        return seen.size() == tasks.size() ? tasks : List.copyOf(seen.values());
    }

    @Override
    protected void processTasks(CancellableTask nodeTask, ListTasksRequest request, ActionListener<List<Task>> nodeOperation) {
        if (request.getWaitForCompletion()) {
            final ListenableActionFuture<List<Task>> future = new ListenableActionFuture<>();
            final List<Task> processedTasks = new ArrayList<>();
            final Set<Task> removedTasks = ConcurrentCollections.newConcurrentSet();
            final Set<Task> matchedTasks = ConcurrentCollections.newConcurrentSet();
            final RefCounted removalRefs = AbstractRefCounted.of(() -> {
                matchedTasks.removeAll(removedTasks);
                removedTasks.clear();
                if (matchedTasks.isEmpty()) {
                    future.onResponse(processedTasks);
                }
            });

            final AtomicBoolean collectionComplete = new AtomicBoolean();
            final RemovedTaskListener removedTaskListener = task -> {
                if (collectionComplete.get() == false && removalRefs.tryIncRef()) {
                    removedTasks.add(task);
                    removalRefs.decRef();
                } else {
                    matchedTasks.remove(task);
                    if (matchedTasks.isEmpty()) {
                        future.onResponse(processedTasks);
                    }
                }
            };
            taskManager.registerRemovedTaskListener(removedTaskListener);
            final ActionListener<List<Task>> allMatchedTasksRemovedListener = ActionListener.runBefore(
                nodeOperation,
                () -> taskManager.unregisterRemovedTaskListener(removedTaskListener)
            );
            try {
                for (final var task : processTasks(request)) {
                    if (task.getAction().startsWith(TYPE.name()) == false) {
                        // It doesn't make sense to wait for List Tasks and it can cause an infinite loop of the task waiting
                        // for itself or one of its child tasks
                        matchedTasks.add(task);
                    }
                    processedTasks.add(task);
                }
            } catch (Exception e) {
                allMatchedTasksRemovedListener.onFailure(e);
                return;
            }
            removalRefs.decRef();
            collectionComplete.set(true);

            final var threadPool = clusterService.threadPool();
            future.addListener(
                new ContextPreservingActionListener<>(
                    threadPool.getThreadContext().newRestorableContext(false),
                    allMatchedTasksRemovedListener
                ),
                threadPool.executor(ThreadPool.Names.MANAGEMENT),
                null
            );
            future.addTimeout(
                requireNonNullElse(request.getTimeout(), DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT),
                threadPool,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            nodeTask.addListener(() -> future.onFailure(new TaskCancelledException("task cancelled")));
        } else {
            super.processTasks(nodeTask, request, nodeOperation);
        }
    }
}
