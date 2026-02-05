/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Integration test(s) for reindex task relocation on node shutdown.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class ReindexRelocationIT extends ESIntegTestCase {

    private static final String SOURCE_INDEX = "reindex_src";
    private static final String DEST_INDEX = "reindex_dst";
    private static final int BULK_SIZE = 1;
    private static final int REQUESTS_PER_SECOND = 1;
    private static final int NUM_OF_SLICES = 2;
    private static final int NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST = 60 * REQUESTS_PER_SECOND * BULK_SIZE;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, ReindexManagementPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int ordinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(ordinal, otherSettings)).build();
    }

    /**
     * Test long-running reindex task is relocated to a suitable node, by doing the following:
     * 1. Create two named data nodes: source_node (hosting the shard) and task_node (hosting the task)
     * 2. Create source index pinned to source_node without replicas, so the scroll always lives there
     * 3. Create destination index
     * 4. Start a throttled reindex on task_node
     * 5. Stop task_node and observe relocation to source_node
     */
    public void testReindexRelocation() throws Exception {
        assumeTrue("reindex resilience is enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);

        final String master = internalCluster().startMasterOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        createSourceIndexPinnedToNode(nodeA);
        createDestinationIndex();
        indexRandom(true, SOURCE_INDEX, NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST);
        ensureGreen(SOURCE_INDEX);

        // Start throttled async reindex on nodeB
        final TaskId originalTaskId = startAsyncThrottledReindex(nodeB);

        final TaskInfo running = getRunningTask(originalTaskId);
        final var reindexNode = nodeById(running.taskId().getNodeId());
        assertThat("task should start on nodeB", reindexNode.getName(), equalTo(nodeB));

        // Stop the node hosting the task to trigger relocation
        internalCluster().getInstance(ShutdownPrepareService.class, nodeB).prepareForShutdown();
        internalCluster().stopNode(nodeB);

        //todo(szy): finish off test
        final TaskGroup relocatedParent = assertRelocatedParentTask(reindexNode.getId());

        // Speed it up post-relocation to keep the test fast
        rethrottle(relocatedParent.taskInfo().taskId().toString(), -1); // unlimited

        // Wait for completion of the relocated task
        final var relocatedResult = getCompletedTaskResult(relocatedParent.taskInfo().taskId());
        assertNotNull(relocatedResult.getTask());
        assertNull("relocated task should not have error", relocatedResult.getError());

        // Assert destination index has all documents reindexed
        assertBusy(() -> assertDocCount(DEST_INDEX, NUMBER_OF_DOCUMENTS_THAT_TAKES_60_SECONDS_TO_INGEST), 60, TimeUnit.SECONDS);

        // Original task should be recorded as failed (with error) in tasks index
        final var originalResult = getCompletedTaskResult(originalTaskId);
        assertNotNull(originalResult.getTask());
        assertNotNull("original task should have failed with an error due to relocation", originalResult.getError());
    }

    private TaskGroup assertRelocatedParentTask(final String excludedNodeId) throws Exception {
        final TaskGroup[] relocatedParentHolder = new TaskGroup[1];
        assertBusy(() -> {
            final var maybeParent = findAnyRunningReindexParentExcludingNode(excludedNodeId);
            assertTrue("expected relocated parent task", maybeParent.isPresent());
            final var parent = maybeParent.get();
            assertThat("expected two slice subtasks", parent.childTasks().size(), equalTo(NUM_OF_SLICES));
            final String newNodeId = parent.taskInfo().taskId().getNodeId();
            parent.childTasks().forEach(child -> assertThat(child.taskInfo().taskId().getNodeId(), equalTo(newNodeId)));
            relocatedParentHolder[0] = parent;
        }, 60, TimeUnit.SECONDS);
        return relocatedParentHolder[0];
    }

    private TaskId startAsyncThrottledReindex(final String nodeName) throws Exception {
        try (RestClient restClient = createRestClient(nodeName)) {
            final Request request = new Request("POST", "/_reindex");
            request.addParameter("wait_for_completion", "false");
            request.addParameter("slices", Integer.toString(NUM_OF_SLICES));
            request.addParameter("requests_per_second", Integer.toString(REQUESTS_PER_SECOND));
            request.setJsonEntity(Strings.format("""
                {
                  "source": {
                    "index": "%s",
                    "size": %d
                  },
                  "dest": {
                    "index": "%s"
                  }
                }
                """, SOURCE_INDEX, BULK_SIZE, DEST_INDEX));

            final Response response = restClient.performRequest(request);
            final String task = (String) ESRestTestCase.entityAsMap(response).get("task");
            assertNotNull("reindex did not return a task id", task);
            return new TaskId(task);
        }
    }

    private TaskInfo getRunningTask(final TaskId taskId) {
        final GetTaskResponse response = clusterAdmin().prepareGetTask(taskId).get();
        final TaskResult task = response.getTask();
        assertNotNull(task);
        assertThat(task.isCompleted(), is(false));
        return task.getTask();
    }

    private GetTaskResponse findFinishedTask(final TaskId originalTaskId) {
        return clusterAdmin().prepareGetTask(originalTaskId).setWaitForCompletion(true).get();
    }

    private Optional<TaskGroup> findAnyRunningReindexParentExcludingNode(final String excludedNodeId) {
        final ListTasksResponse response = clusterAdmin().prepareListTasks().setActions(ReindexAction.NAME).setDetailed(true).get();
        return response.getTaskGroups().stream().filter(g -> g.taskInfo().taskId().getNodeId().equals(excludedNodeId) == false).findFirst();
    }

    private TaskResult getCompletedTaskResult(final TaskId taskId) {
        final GetTaskResponse response = clusterAdmin().prepareGetTask(taskId).setWaitForCompletion(true).get();
        final TaskResult task = response.getTask();
        assertNotNull(task);
        assertThat(task.isCompleted(), is(true));
        return task;
    }

    private void createSourceIndexPinnedToNode(final String nodeName) {
        prepareCreate(SOURCE_INDEX).setSettings(
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.routing.allocation.require._name", nodeName)
        ).get();
        ensureGreen(TimeValue.timeValueSeconds(10), SOURCE_INDEX);
    }

    private void createDestinationIndex() {
        prepareCreate(DEST_INDEX).setSettings(
            Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)
        ).get();
        ensureGreen(TimeValue.timeValueSeconds(10), DEST_INDEX);
    }

    private void rethrottle(final String taskIdString, final int rps) {
        try {
            final RestClient restClient = getRestClient();
            final Request request = new Request("POST", "/_reindex/" + taskIdString + "/_rethrottle");
            request.addParameter("requests_per_second", Integer.toString(rps));
            restClient.performRequest(request);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private DiscoveryNode nodeById(final String nodeId) {
        final DiscoveryNode node = clusterService().state().nodes().get(nodeId);
        assertNotNull("node must be found", node);
        return node;
    }

    private void assertDocCount(final String index, final int expected) {
        final TimeValue timeout = TimeValue.THIRTY_SECONDS;
        final var resp = client().admin().indices().prepareGetIndex(timeout).addIndices(index).get();
        // Use _count API via REST for simplicity
        try {
            final RestClient restClient = getRestClient();
            final Request request = new Request("GET", "/" + index + "/_count");
            final Response response = restClient.performRequest(request);
            final Map<?, ?> body = ESRestTestCase.entityAsMap(response);
            final int count = ((Number) body.get("count")).intValue();
            assertThat(count, equalTo(expected));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
