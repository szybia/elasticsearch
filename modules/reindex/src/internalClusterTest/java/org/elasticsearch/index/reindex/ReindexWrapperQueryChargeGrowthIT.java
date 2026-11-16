/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.RethrottleRequestBuilder;
import org.elasticsearch.reindex.management.ReindexManagementPlugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/// Shows that a `_reindex` started with `wait_for_completion=false` keeps adding to the held parsed-request charge while it runs, when
/// its query is a `wrapper` query and it paginates with a point-in-time. Every batch is a new search, the coordinator parses the
/// `wrapper` query again for each one, and that parse charges the REST request's reservation, which the task holds until it ends.
public class ReindexWrapperQueryChargeGrowthIT extends ESIntegTestCase {

    private static final String SOURCE = "source";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // The management plugin registers the feature that turns point-in-time pagination on.
        return List.of(ReindexPlugin.class, ReindexManagementPlugin.class, TestTelemetryPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // The test cluster can make a node's request breaker a noop one, which charges nothing.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            .build();
    }

    /// Passes while the problem exists: the held bytes grow as batches run.
    public void testHeldChargeGrowsWithEveryBatchOfAWrapperQuery() throws Exception {
        List<Long> held = heldBytesAsBatchesRun(true);
        assertThat(held.get(1), greaterThan(held.get(0)));
        assertThat(held.get(2), greaterThan(held.get(1)));
    }

    /// The control: the same query sent plainly is parsed once, by the REST handler, so the held bytes stay flat.
    public void testHeldChargeIsFlatWithoutAWrapperQuery() throws Exception {
        List<Long> held = heldBytesAsBatchesRun(false);
        assertThat(held.get(1), equalTo(held.get(0)));
        assertThat(held.get(2), equalTo(held.get(1)));
    }

    /// Starts a reindex and returns the held bytes at three points: throttled after its first batch, then after 20 and 60 batches.
    private List<Long> heldBytesAsBatchesRun(boolean wrapped) throws Exception {
        assertTrue(
            "reindex has to paginate with a point-in-time",
            internalCluster().getCurrentMasterNodeInstance(FeatureService.class)
                .clusterHasFeature(clusterService().state(), ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE)
        );
        String field = randomAlphaOfLength(8);
        // Lower case, so that a term query matches the analyzed value.
        String value = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        // One doc per batch, and far more docs than the batches sampled, so the task is still running at the last sample.
        createIndex(SOURCE);
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            docs.add(prepareIndex(SOURCE).setId(Integer.toString(i)).setSource(field, value));
        }
        indexRandom(true, docs);

        // Many clauses, so that one parse of the query is a visible charge.
        BoolQueryBuilder inner = QueryBuilders.boolQuery();
        for (int i = between(50, 200); i > 0; i--) {
            inner.should(QueryBuilders.termQuery(field, value));
        }
        QueryBuilder query = wrapped ? QueryBuilders.wrapperQuery(Strings.toString(inner)) : inner;

        Request request = new Request("POST", "/_reindex");
        request.addParameter("wait_for_completion", "false");
        request.addParameter("requests_per_second", "0.000001");
        request.setJsonEntity(Strings.format("""
            {"source":{"index":"%s","size":1,"query":%s},"dest":{"index":"%s"}}""", SOURCE, Strings.toString(query), randomIdentifier()));
        Response response = getRestClient().performRequest(request);
        final TaskId taskId;
        try (InputStream content = response.getEntity().getContent()) {
            taskId = new TaskId((String) XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false).get("task"));
        }

        List<Long> held = new ArrayList<>();
        assertBusy(() -> assertThat(status(taskId).getThrottledUntil().nanos(), greaterThan(0L)));
        held.add(sample(taskId, wrapped));

        new RethrottleRequestBuilder(client()).setTargetTaskId(taskId).setRequestsPerSecond(20f).get().rethrowFailures("Rethrottle");
        for (int batches : new int[] { 20, 60 }) {
            assertBusy(() -> assertThat(status(taskId).getBatches(), greaterThanOrEqualTo(batches)), 30, TimeUnit.SECONDS);
            held.add(sample(taskId, wrapped));
        }

        clusterAdmin().prepareCancelTasks().setTargetTaskId(taskId).get();
        clusterAdmin().prepareGetTask(taskId).setWaitForCompletion(true).setTimeout(TimeValue.timeValueSeconds(30)).get();
        assertBusy(() -> assertThat(parsedRequestHeldBytes(), equalTo(0L)));
        return held;
    }

    private long sample(TaskId taskId, boolean wrapped) {
        long held = parsedRequestHeldBytes();
        logger.info("--> wrapped [{}] batches [{}] held bytes [{}]", wrapped, status(taskId).getBatches(), held);
        return held;
    }

    private BulkByPaginatedSearchTask.Status status(TaskId taskId) {
        return (BulkByPaginatedSearchTask.Status) clusterAdmin().prepareGetTask(taskId).get().getTask().getTask().status();
    }

    /// The bytes the request breakers of all nodes hold for parsed requests, from the held memory metric.
    private long parsedRequestHeldBytes() {
        long held = 0;
        for (String node : internalCluster().getNodeNames()) {
            TestTelemetryPlugin telemetry = internalCluster().getInstance(PluginsService.class, node)
                .filterPlugins(TestTelemetryPlugin.class)
                .findFirst()
                .orElseThrow();
            for (Measurement measurement : telemetry.getLongUpDownCounterMeasurement(CircuitBreakerMetrics.ES_BREAKER_MEMORY_HELD)) {
                Map<String, Object> attributes = measurement.attributes();
                if (CircuitBreaker.REQUEST.equals(attributes.get(ChildMemoryCircuitBreaker.BREAKER_METRIC_TYPE_ATTRIBUTE))
                    && ChildMemoryCircuitBreaker.CATEGORY_PARSED_REQUEST.equals(
                        attributes.get(ChildMemoryCircuitBreaker.CIRCUIT_BREAKER_CATEGORY_ATTRIBUTE)
                    )) {
                    held += measurement.getLong();
                }
            }
        }
        return held;
    }
}
