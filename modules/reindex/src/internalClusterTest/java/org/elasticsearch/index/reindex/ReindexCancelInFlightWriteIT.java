/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.management.GetReindexRequest;
import org.elasticsearch.reindex.management.ReindexManagementPlugin;
import org.elasticsearch.reindex.management.TransportGetReindexAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/// Cancels a reindex while its destination bulk is between two shard dispatches and asserts that the outcome is the same as for a
/// cancel landing anywhere else in the reindex: a response rather than a failure, the cancel reason in the status, no bulk failures,
/// and only the documents of the shard request that was already in flight written.
///
/// `BulkOperation` dispatches one shard level request per destination shard. Once the parent task has banned its children, dispatching
/// a later shard fails synchronously in [org.elasticsearch.tasks.TaskManager#registerChildConnection]. That failure used to escape the
/// dispatch loop and fail the bulk listener while the earlier shard request was still writing. Reindex aliases pooled search hit bytes
/// into its index requests and releases them when the bulk listener completes, so the still running write then read freed memory. The
/// bulk now treats the rejected dispatch as a failure of that shard's items and completes only once every dispatched shard has
/// finished, which is what this test pins.
///
/// Four things have to be true at once for that shared memory to exist, and for the ban to have a later dispatch to reject:
///
///  - The destination bulk covers two or more shards, so the dispatch loop still has a shard left when the ban lands. With one
///    destination shard there is no second dispatch and nothing is rejected.
///  - The destination primary is on the coordinating node. `TransportService#sendLocalRequest` hands the request object straight
///    to the handler, so the `IndexRequest` reaches the write thread still pointing at the search hit bytes. A remote primary
///    serializes the request instead, and the write then reads a copy owned by the inbound transport buffer, which the reindex
///    does not free.
///  - The source search runs its fetch as a separate phase, which needs two or more source shards. When
///    `SearchService#executeQueryPhase` sees `request.numberOfShards() == 1` it runs the fetch inline against the same context,
///    so the search never reaches `SearchTransportService#sendExecuteFetch` and never produces pooled hits.
///  - That separate fetch phase takes the chunked path, which serializes hits into a pooled buffer on the data node and gives
///    the coordinator retained [ReleasableBytesReference] slices of it rather than copies. `SearchTransportService#sendExecuteFetch`
///    excludes cross-cluster search, scroll, and data nodes older than `CHUNKED_FETCH_DOC_ID_ORDER`, and is gated on
///    `search.fetch_phase_chunked_enabled`. Reindex paginates with a point-in-time reader rather than a scroll, so it is not
///    excluded.
///
/// One node with two shards per index satisfies all four requirements.
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ReindexCancelInFlightWriteIT extends ESIntegTestCase {

    private static final String SOURCE_INDEX = "reindex_cancel_src";
    private static final String DESTINATION_INDEX = "reindex_cancel_dst";

    /// More than one source shard so the fetch runs as a separate chunked phase and the hits arrive as pooled slices.
    private static final int SOURCE_SHARDS = 2;
    /// More than one destination shard so the dispatch loop has a second shard left to reject once the ban lands.
    private static final int DESTINATION_SHARDS = 2;
    private static final int DOC_COUNT = 200;
    /// How long to wait for write to be released before failing the test, to not hang the suite.
    private static final TimeValue PARKED_WRITE_TIMEOUT = TimeValue.timeValueSeconds(15);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, ReindexManagementPlugin.class, CancelOrchestrationPlugin.class);
    }

    @Before
    public void resetOrchestration() {
        CancelOrchestrationPlugin.reset();
    }

    public void testCancelBetweenDestinationShardDispatches() throws Exception {
        createTestIndex(SOURCE_INDEX, SOURCE_SHARDS);
        createTestIndex(DESTINATION_INDEX, DESTINATION_SHARDS);

        indexRandom(true, SOURCE_INDEX, DOC_COUNT);

        final ReindexRequestBuilder reindex = new ReindexRequestBuilder(client()).source(SOURCE_INDEX)
            .destination(DESTINATION_INDEX)
            .setShouldStoreResult(true);
        // A single batch covering every document, so it necessarily spans both destination shards
        reindex.source().setSize(DOC_COUNT);

        final ActionFuture<BulkByPaginatedSearchResponse> reindexFuture = client().execute(ReindexAction.INSTANCE, reindex.request());

        // The first destination shard request is now parked in the engine and the dispatch loop is blocked before the second one
        safeAwait(CancelOrchestrationPlugin.destinationWriteParked);

        final TaskId reindexTaskId = CancelOrchestrationPlugin.reindexTaskId.get();
        assertTrue("the reindex task id was never captured, so there is nothing to cancel", reindexTaskId.isSet());
        try {
            // Without wait_for_completion this returns once the bans are in place on every descendant, so the bulk task's children are
            // banned by the time the dispatch loop is let go and its next dispatch is rejected
            clusterAdmin().prepareCancelTasks().setTargetTaskId(reindexTaskId).get();
        } finally {
            CancelOrchestrationPlugin.cancelLanded.countDown();
        }

        assertFalse("the reindex completed while its first destination shard request was still writing", reindexFuture.isDone());

        CancelOrchestrationPlugin.releaseDestinationWrite.countDown();
        safeAwait(CancelOrchestrationPlugin.destinationWriteReleased);
        final BulkByPaginatedSearchResponse response = reindexFuture.actionGet(SAFE_AWAIT_TIMEOUT);

        logger.info(
            "destination bulk had [{}] items, its first shard request had [{}], [{}] destination shard requests were dispatched, "
                + "reindex responded with [{}]",
            CancelOrchestrationPlugin.destinationBulkItemCount.get(),
            CancelOrchestrationPlugin.firstShardRequestItemCount.get(),
            CancelOrchestrationPlugin.destinationShardRequestsDispatched.get(),
            response
        );

        final String premiseFailureMessage = CancelOrchestrationPlugin.premiseFailure.get();
        if (premiseFailureMessage != null) {
            fail("a reproduction premise broke, so nothing was proven: " + premiseFailureMessage);
        }
        assertThat(
            "every document has to land in one bulk, otherwise the item counts below are comparing across batches",
            CancelOrchestrationPlugin.destinationBulkItemCount.get(),
            equalTo(DOC_COUNT)
        );
        final int firstShardRequestItemCount = CancelOrchestrationPlugin.firstShardRequestItemCount.get();
        assertThat(
            "the first destination shard request has to cover only part of the bulk, otherwise the dispatch loop has no second shard "
                + "left for the ban to reject",
            firstShardRequestItemCount,
            lessThan(DOC_COUNT)
        );
        assertThat(
            "a dispatch rejected by the ban never reaches the shard action, so exactly one destination shard request may have got there",
            CancelOrchestrationPlugin.destinationShardRequestsDispatched.get(),
            equalTo(1)
        );

        assertFalse(
            "destination write resumed after the reindexed document source had been released",
            CancelOrchestrationPlugin.sourceReleasedUnderWrite.get()
        );

        // Same shape as a cancel landing anywhere else in the reindex: the reason is in the status, the rejected shard's item failures
        // are not surfaced, and the counters reflect the shard request that did complete
        assertThat(response.getReasonCancelled(), equalTo(CancelTasksRequest.DEFAULT_REASON));
        assertThat(response.getBulkFailures(), empty());
        assertThat(response.getSearchFailures(), empty());
        assertThat(response.getTotal(), equalTo((long) DOC_COUNT));
        assertThat(response.getBatches(), equalTo(1));
        assertThat(response.getCreated(), equalTo((long) firstShardRequestItemCount));

        assertStoredTaskOutcome(reindexTaskId, firstShardRequestItemCount);

        // The shard request that was in flight when the cancel landed finished, the rejected one never wrote anything
        indicesAdmin().prepareRefresh(DESTINATION_INDEX).get();
        assertHitCount(prepareSearch(DESTINATION_INDEX).setSize(0).setTrackTotalHits(true), firstShardRequestItemCount);
    }

    /// Asserts the terminal task document behind `GET _tasks/{id}` and `GET _reindex/{id}`, which is all a caller has left once the
    /// reindex call itself has returned. Both APIs read the same stored `TaskResult`, and `_reindex` only re-renders the task header
    /// through its own allowlist, so their outcome halves are expected to be identical.
    private void assertStoredTaskOutcome(TaskId reindexTaskId, int created) {
        final GetReindexRequest getReindex = new GetReindexRequest(reindexTaskId, false, null);
        final Map<String, Object> tasksBody = renderAndLog("_tasks/" + reindexTaskId, clusterAdmin().prepareGetTask(reindexTaskId).get());
        final Map<String, Object> reindexBody = renderAndLog(
            "_reindex/" + reindexTaskId,
            client().execute(TransportGetReindexAction.TYPE, getReindex).actionGet(SAFE_AWAIT_TIMEOUT)
        );

        assertThat(tasksBody.get("completed"), equalTo(true));
        assertThat(tasksBody, not(hasKey("error")));
        assertThat(tasksBody, hasKey("response"));

        assertThat(
            reindexBody.keySet(),
            containsInAnyOrder(
                "completed",
                "id",
                "description",
                "start_time_in_millis",
                "running_time_in_nanos",
                "cancelled",
                "status",
                "response"
            )
        );
        assertThat(reindexBody, not(hasKey("error")));
        assertThat(reindexBody.get("completed"), equalTo(true));
        assertThat(reindexBody.get("id"), equalTo(reindexTaskId.toString()));
        assertThat(reindexBody.get("description"), equalTo("reindex from [" + SOURCE_INDEX + "] to [" + DESTINATION_INDEX + "]"));
        // .tasks system index doesn't store this, so it's confusingly false
        assertThat(reindexBody.get("cancelled"), equalTo(false));
        assertThat("the two APIs must not disagree about the outcome", reindexBody.get("response"), equalTo(tasksBody.get("response")));

        final Map<String, Object> status = ObjectPath.eval("status", reindexBody);
        assertThat(status.get("total"), equalTo(DOC_COUNT));
        assertThat(status.get("batches"), equalTo(1));
        assertThat(status.get("created"), equalTo(created));
        assertThat(status.get("canceled"), equalTo(CancelTasksRequest.DEFAULT_REASON));

        final Map<String, Object> storedResponse = ObjectPath.eval("response", reindexBody);
        assertThat(storedResponse.get("created"), equalTo(created));
        assertThat(storedResponse.get("canceled"), equalTo(CancelTasksRequest.DEFAULT_REASON));
        assertThat(storedResponse.get("failures"), equalTo(List.of()));
    }

    /// Renders a management API response the way the REST layer does and returns it as a map, logging the body so the whole outcome shows
    /// up in the test output rather than only the fields asserted below.
    private Map<String, Object> renderAndLog(String api, ToXContentObject response) {
        final String body = Strings.toString(response, true, false);
        logger.info("GET {} returned:\n{}", api, body);
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), body, false);
    }

    private void createTestIndex(String index, int numberOfShards) {
        prepareCreate(index).setSettings(indexSettings(numberOfShards, 0)).get();
        ensureGreen(index);
    }

    /// Interleaves the two events the scenario needs: a destination primary write parked while it still points at the reindexed
    /// document's pooled source bytes, and a cancel of the reindex task landing between two shard dispatches of the same bulk.
    public static class CancelOrchestrationPlugin extends Plugin implements ActionPlugin {

        static volatile CountDownLatch destinationWriteParked;
        static volatile CountDownLatch releaseDestinationWrite;
        static volatile CountDownLatch destinationWriteReleased;
        /// Counted down by the test once the cancel API has returned, which is what lets the dispatch loop continue to the shard the ban
        /// then rejects.
        static volatile CountDownLatch cancelLanded;

        static final AtomicBoolean firstWriteSeen = new AtomicBoolean();
        static final AtomicBoolean sourceReleasedUnderWrite = new AtomicBoolean();
        static final AtomicReference<String> premiseFailure = new AtomicReference<>();
        static final AtomicInteger destinationBulkItemCount = new AtomicInteger();
        static final AtomicInteger destinationShardRequestsDispatched = new AtomicInteger();
        static final AtomicInteger firstShardRequestItemCount = new AtomicInteger();
        /// Captured while the reindex is running, because the client call does not hand one back and both the cancel and the stored
        /// outcome are addressed by task id.
        static final AtomicReference<TaskId> reindexTaskId = new AtomicReference<>(TaskId.EMPTY_TASK_ID);

        static void reset() {
            destinationWriteParked = new CountDownLatch(1);
            releaseDestinationWrite = new CountDownLatch(1);
            destinationWriteReleased = new CountDownLatch(1);
            cancelLanded = new CountDownLatch(1);
            firstWriteSeen.set(false);
            sourceReleasedUnderWrite.set(false);
            premiseFailure.set(null);
            destinationBulkItemCount.set(0);
            destinationShardRequestsDispatched.set(0);
            firstShardRequestItemCount.set(0);
            reindexTaskId.set(TaskId.EMPTY_TASK_ID);
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (DESTINATION_INDEX.equals(indexModule.getIndex().getName())) {
                indexModule.addIndexOperationListener(new ParkFirstDestinationWrite());
            }
        }

        @Override
        public Collection<MappedActionFilter> getMappedActionFilters() {
            return List.of(new RecordDestinationBulkSize(), new HoldDispatchLoopUntilCancelled());
        }

        static void recordPremiseFailure(String message) {
            premiseFailure.compareAndSet(null, message);
        }

        /// Neither hook may use `safeAwait`. It fails with an [AssertionError], which is not caught by the runnables driving the write
        /// path or by the action filter chain, so a timeout would abandon a shard permit and hang the whole suite instead of reporting
        /// the premise that broke.
        static boolean awaitQuietly(CountDownLatch latch, TimeValue timeout) {
            try {
                return latch.await(timeout.millis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        /// Records how many items the reindex put in one bulk, which is what tells us whether a single shard request could have covered
        /// the whole batch, and the id of the reindex task that issued it.
        private static class RecordDestinationBulkSize implements MappedActionFilter {

            @Override
            public String actionName() {
                return TransportBulkAction.NAME;
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                Task task,
                String action,
                Request request,
                ActionListener<Response> listener,
                ActionFilterChain<Request, Response> chain
            ) {
                if (request instanceof BulkRequest bulkRequest
                    && bulkRequest.requests().isEmpty() == false
                    && DESTINATION_INDEX.equals(bulkRequest.requests().get(0).index())) {
                    destinationBulkItemCount.compareAndSet(0, bulkRequest.numberOfActions());
                    // Reindex defaults to one slice, so the bulk's parent is the unsliced task the client is waiting on rather than a
                    // slice worker, which is what the cancel and GET _reindex/{id} need
                    reindexTaskId.compareAndSet(TaskId.EMPTY_TASK_ID, task.getParentTaskId());
                }
                chain.proceed(task, action, request, listener);
            }
        }

        /// Holds the first destination primary write open while the reindex is cancelled, then records whether the document source it
        /// is about to index was freed underneath it.
        private static class ParkFirstDestinationWrite implements IndexingOperationListener {

            @Override
            public Engine.Index preIndex(ShardId shardId, Engine.Index index) {
                if (index.origin() != Engine.Operation.Origin.PRIMARY || firstWriteSeen.compareAndSet(false, true) == false) {
                    return index;
                }
                final BytesReference source = index.parsedDoc().source().originalBytes();
                try {
                    destinationWriteParked.countDown();
                    if (awaitQuietly(releaseDestinationWrite, PARKED_WRITE_TIMEOUT) == false) {
                        recordPremiseFailure("the parked destination write was never released");
                    } else if (source instanceof ReleasableBytesReference pooled) {
                        // hasReferences() is the only accessor that stays safe to call once the bytes have been released
                        sourceReleasedUnderWrite.set(pooled.hasReferences() == false);
                    } else {
                        recordPremiseFailure(
                            "the reindexed document source was not pooled, so it cannot be freed under the write: "
                                + source.getClass().getSimpleName()
                        );
                    }
                } finally {
                    destinationWriteReleased.countDown();
                }
                return index;
            }
        }

        /// Runs inline on the thread driving `BulkOperation`'s dispatch loop, which makes it the only hook available between two shard
        /// dispatches of the same bulk. Once the first destination shard is writing, it holds the loop until the test has cancelled
        /// the reindex, so the next dispatch is rejected in [org.elasticsearch.tasks.TaskManager#registerChildConnection] by the ban
        /// that cancel put in place. A rejected dispatch never reaches this filter, which is how the dispatch count below tells the
        /// two shards apart.
        private static class HoldDispatchLoopUntilCancelled implements MappedActionFilter {

            @Override
            public String actionName() {
                return TransportShardBulkAction.ACTION_NAME;
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                Task task,
                String action,
                Request request,
                ActionListener<Response> listener,
                ActionFilterChain<Request, Response> chain
            ) {
                if (request instanceof BulkShardRequest shardRequest
                    && DESTINATION_INDEX.equals(shardRequest.index())
                    && destinationShardRequestsDispatched.incrementAndGet() == 1) {
                    firstShardRequestItemCount.set(shardRequest.items().length);
                    // TransportShardBulkAction registers its primary handler with the write pool, and
                    // TransportReplicationAction#handlePrimaryRequest dispatches to it before taking a permit, so this call returns
                    // once the operation is queued rather than once it is written. That is what frees this thread to hold the loop
                    // while the first shard is still writing.
                    chain.proceed(task, action, request, listener);
                    if (awaitQuietly(destinationWriteParked, SAFE_AWAIT_TIMEOUT) == false) {
                        recordPremiseFailure("the first destination shard request never reached the engine");
                    } else if (awaitQuietly(cancelLanded, SAFE_AWAIT_TIMEOUT) == false) {
                        recordPremiseFailure("the reindex was never cancelled while the dispatch loop was held");
                    }
                    return;
                }
                chain.proceed(task, action, request, listener);
            }
        }
    }
}
