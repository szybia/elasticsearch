/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Verifies that the double-broadcast list never returns an empty result while a reindex task
 * is being relocated from one node to another.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ReindexListRelocationRaceIT extends ESIntegTestCase {

    private static final String SOURCE_INDEX = "reindex_src";
    private static final String DEST_INDEX = "reindex_dst";

    private final int bulkSize = randomIntBetween(1, 4);
    private final int numOfSlices = randomIntBetween(1, 4);
    private final int requestsPerSecond = randomIntBetween(bulkSize * numOfSlices, 20);
    private final int numberOfDocumentsThatTakes60SecondsToIngest = 60 * requestsPerSecond;

    @BeforeClass
    public static void skipSetupIfReindexResilienceDisabled() {
        assumeTrue("reindex resilience is enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, ReindexManagementPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testListNeverReturnsEmptyDuringRelocation() throws Exception {
        final String nodeAName = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        final String nodeBName = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        ensureStableCluster(2);

        createIndexPinnedToNodeName(SOURCE_INDEX, nodeAName);
        createIndexPinnedToNodeName(DEST_INDEX, nodeAName);
        indexRandom(true, SOURCE_INDEX, numberOfDocumentsThatTakes60SecondsToIngest);
        ensureGreen(SOURCE_INDEX, DEST_INDEX);

        final TaskId originalTaskId = startAsyncThrottledReindexOnNode(nodeBName);

        // Wait until the list API shows the task before we start the race
        assertBusy(() -> {
            final ListReindexResponse response = client().execute(
                TransportListReindexAction.TYPE,
                new ListReindexRequest().setActions(ReindexAction.NAME)
            ).actionGet();
            assertFalse("reindex task should be listed", response.getTasks().isEmpty());
        });

        final AtomicBoolean sawEmpty = new AtomicBoolean(false);
        final AtomicBoolean keepRunning = new AtomicBoolean(true);
        final AtomicReference<Exception> threadException = new AtomicReference<>();

        final Thread listThread = new Thread(() -> {
            try {
                while (keepRunning.get()) {
                    final ListReindexResponse response = client().execute(
                        TransportListReindexAction.TYPE,
                        new ListReindexRequest().setActions(ReindexAction.NAME)
                    ).actionGet();
                    if (response.getTasks().isEmpty()) {
                        sawEmpty.set(true);
                    }
                }
            } catch (Exception e) {
                threadException.set(e);
            }
        }, "reindex-list-race-thread");
        listThread.start();

        try {
            // Request relocation on all reindex tasks running on nodeB
            final TaskManager taskManager = internalCluster().getInstance(TaskManager.class, nodeBName);
            for (Task task : taskManager.getTasks().values()) {
                if (task instanceof BulkByScrollTask bbs
                    && bbs.isEligibleForRelocationOnShutdown()
                    && bbs.isRelocationRequested() == false) {
                    bbs.requestRelocation();
                }
            }

            // Wait for relocation to complete: the original task result is stored in .tasks
            assertBusy(
                () -> assertTrue(".tasks index should exist after relocation", indexExists(TaskResultsService.TASK_INDEX)),
                30,
                TimeUnit.SECONDS
            );

            // Keep listing for a bit after relocation to cover any trailing race window
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        } finally {
            keepRunning.set(false);
            listThread.join(TimeUnit.SECONDS.toMillis(30));
        }

        if (threadException.get() != null) {
            throw new AssertionError("list thread threw an exception", threadException.get());
        }
        assertFalse("list returned empty during relocation", sawEmpty.get());

        // Clean up: unthrottle the relocated task so it finishes quickly
        final GetReindexResponse getResponse = client().execute(
            TransportGetReindexAction.TYPE,
            new GetReindexRequest(originalTaskId, false, TimeValue.timeValueSeconds(30))
        ).actionGet();
        final TaskId relocatedTaskId = getResponse.getRelocatedTask()
            .map(r -> r.getTask().taskId())
            .orElseThrow(() -> new AssertionError("expected relocated task"));
        unthrottleReindex(relocatedTaskId);

        client().execute(TransportGetReindexAction.TYPE, new GetReindexRequest(originalTaskId, true, TimeValue.timeValueSeconds(60)))
            .actionGet();
    }

    private TaskId startAsyncThrottledReindexOnNode(final String nodeName) throws Exception {
        try (RestClient restClient = createRestClient(nodeName)) {
            final Request request = new Request("POST", "/_reindex");
            request.addParameter("wait_for_completion", "false");
            request.addParameter("slices", Integer.toString(numOfSlices));
            request.addParameter("requests_per_second", Integer.toString(requestsPerSecond));
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
                """, SOURCE_INDEX, bulkSize, DEST_INDEX));

            final Response response = restClient.performRequest(request);
            final String task = (String) ESRestTestCase.entityAsMap(response).get("task");
            assertNotNull("reindex did not return a task id", task);
            return new TaskId(task);
        }
    }

    private void unthrottleReindex(final TaskId taskId) throws Exception {
        final Request request = new Request("POST", "/_reindex/" + taskId + "/_rethrottle");
        request.addParameter("requests_per_second", Integer.toString(-1));
        getRestClient().performRequest(request);
    }

    private void createIndexPinnedToNodeName(final String index, final String nodeName) {
        prepareCreate(index).setSettings(
            Settings.builder()
                .put("index.number_of_shards", randomIntBetween(1, 3))
                .put("index.number_of_replicas", 0)
                .put("index.routing.allocation.require._name", nodeName)
        ).get();
        ensureGreen(TimeValue.timeValueSeconds(10), index);
    }
}
