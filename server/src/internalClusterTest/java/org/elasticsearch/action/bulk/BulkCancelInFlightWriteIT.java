/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

/// Cancels a bulk while it is between two shard dispatches and asserts that the bulk still completes like any other bulk with a failed
/// shard: with a response once every dispatched shard request has finished, the rejected shard's items failed, and the in-flight
/// shard's items written.
///
/// A bulk request may alias pooled bytes that its caller owns, and a caller is entitled to let go of those bytes once the bulk listener
/// has completed. Once the bulk task's children are banned, dispatching a later shard fails synchronously in
/// [org.elasticsearch.tasks.TaskManager#registerChildConnection]. That failure used to escape the dispatch loop and fail the bulk
/// listener while an earlier shard request was still writing, so the caller freed the bytes that write was reading.
///
/// This test plays the caller, so it owns the pooled source bytes outright and can watch exactly when they are freed. Two shards on
/// a single node are what the scenario needs: two so the dispatch loop still has a shard left to reject once the ban lands, and one
/// node so that `TransportService#sendLocalRequest` hands the request object to the write thread by reference rather than a
/// serialized copy of it.
///
/// `ReindexCancelInFlightWriteIT`, in the reindex module, covers the same contract with a real caller whose bytes come from a
/// search fetch.
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class BulkCancelInFlightWriteIT extends ESIntegTestCase {

    private static final String INDEX = "bulk_cancel_idx";
    /// More than one shard so the dispatch loop has a second shard left to reject once the ban lands.
    private static final int SHARDS = 2;
    private static final int DOC_COUNT = 100;
    private static final String CANCELLATION_MESSAGE = "parent task was cancelled [" + CancelTasksRequest.DEFAULT_REASON + "]";
    /// How long to wait for write to be released before failing the test, to not hang the suite.
    private static final TimeValue PARKED_WRITE_TIMEOUT = TimeValue.timeValueSeconds(15);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(CancelOrchestrationPlugin.class);
    }

    @Before
    public void resetOrchestration() {
        CancelOrchestrationPlugin.reset();
    }

    public void testCancelBetweenShardDispatches() throws Exception {
        // An explicit mapping keeps a dynamic mapping update from having to reach the master while the bulk task's children are banned
        prepareCreate(INDEX).setSettings(indexSettings(SHARDS, 0)).setMapping("field", "type=keyword").get();
        ensureGreen(INDEX);

        final AtomicInteger releases = new AtomicInteger();
        final List<ReleasableBytesReference> sources = new ArrayList<>(DOC_COUNT);
        final BulkRequest bulkRequest = new BulkRequest();
        // One update item so the scenario covers requests that carry their source somewhere other than an IndexRequest of their own
        final int updateSlot = randomIntBetween(0, DOC_COUNT - 1);
        for (int i = 0; i < DOC_COUNT; i++) {
            ReleasableBytesReference source = pooled(new BytesArray("{\"field\":\"" + randomAlphaOfLength(20) + "\"}"), releases);
            sources.add(source);
            if (i == updateSlot) {
                bulkRequest.add(new UpdateRequest(INDEX, Integer.toString(i)).doc(source, XContentType.JSON).docAsUpsert(true));
            } else {
                bulkRequest.add(new IndexRequest(INDEX).id(Integer.toString(i)).source(source, XContentType.JSON));
            }
        }

        final PlainActionFuture<BulkResponse> bulkFuture = new PlainActionFuture<>();
        // Like any real caller, let go of the bytes as soon as the bulk listener completes
        client().execute(TransportBulkAction.TYPE, bulkRequest, ActionListener.releaseBefore(Releasables.wrap(sources), bulkFuture));

        // The first shard request is now parked in the engine and the dispatch loop is blocked before the second one
        safeAwait(CancelOrchestrationPlugin.writeParked);

        final long bulkTaskId = CancelOrchestrationPlugin.bulkTaskId.get();
        assertThat("the bulk task id was never captured, so there is nothing to cancel", bulkTaskId, greaterThanOrEqualTo(0L));
        try {
            // Without wait_for_completion this returns once the bans are in place on every descendant, so the bulk task's children are
            // banned by the time the dispatch loop is let go and its next dispatch is rejected
            clusterAdmin().prepareCancelTasks().setTargetTaskId(new TaskId(clusterService().localNode().getId(), bulkTaskId)).get();
        } finally {
            CancelOrchestrationPlugin.cancelLanded.countDown();
        }

        assertFalse("the bulk completed while its first shard request was still writing", bulkFuture.isDone());

        CancelOrchestrationPlugin.releaseWrite.countDown();
        safeAwait(CancelOrchestrationPlugin.writeReleased);
        final BulkResponse response = bulkFuture.actionGet(SAFE_AWAIT_TIMEOUT);

        logger.info(
            "bulk had [{}] items, its first shard request had [{}], [{}] shard requests were dispatched",
            DOC_COUNT,
            CancelOrchestrationPlugin.firstShardRequestItemCount.get(),
            CancelOrchestrationPlugin.shardRequestsDispatched.get()
        );

        final String premiseFailureMessage = CancelOrchestrationPlugin.premiseFailure.get();
        if (premiseFailureMessage != null) {
            fail("a reproduction premise broke, so nothing was proven: " + premiseFailureMessage);
        }
        final int firstShardRequestItemCount = CancelOrchestrationPlugin.firstShardRequestItemCount.get();
        assertThat(
            "the first shard request has to cover only part of the bulk, otherwise the dispatch loop has no second shard left for the "
                + "ban to reject",
            firstShardRequestItemCount,
            lessThan(DOC_COUNT)
        );
        assertThat(
            "a dispatch rejected by the ban never reaches the shard action, so exactly one shard request may have got there",
            CancelOrchestrationPlugin.shardRequestsDispatched.get(),
            equalTo(1)
        );

        assertFalse(
            "write resumed after the source of the document it was indexing had been released",
            CancelOrchestrationPlugin.sourceReleasedUnderWrite.get()
        );

        // The rejected shard's items fail with the cancellation, the in-flight shard's items succeed
        int succeeded = 0;
        for (BulkItemResponse item : response.getItems()) {
            if (item.isFailed()) {
                assertThat(item.getFailure().getCause(), instanceOf(TaskCancelledException.class));
                assertThat(item.getFailure().getCause().getMessage(), equalTo(CANCELLATION_MESSAGE));
            } else {
                succeeded++;
            }
        }
        assertThat(succeeded, equalTo(firstShardRequestItemCount));

        // The caller's release ran when the listener completed, and by then nothing else was holding the bytes
        assertThat(releases.get(), equalTo(DOC_COUNT));
        sources.forEach(source -> assertFalse(source.hasReferences()));

        indicesAdmin().prepareRefresh(INDEX).get();
        assertHitCount(prepareSearch(INDEX).setSize(0).setTrackTotalHits(true), firstShardRequestItemCount);
    }

    /// Interleaves the two events the scenario needs: a primary write parked while it still points at the caller's pooled source
    /// bytes, and a cancel of the bulk task landing between two shard dispatches of that bulk.
    public static class CancelOrchestrationPlugin extends Plugin implements ActionPlugin {

        static volatile CountDownLatch writeParked;
        static volatile CountDownLatch releaseWrite;
        static volatile CountDownLatch writeReleased;
        /// Counted down by the test once the cancel API has returned, which is what lets the dispatch loop continue to the shard the ban
        /// then rejects.
        static volatile CountDownLatch cancelLanded;

        static final AtomicBoolean firstWriteSeen = new AtomicBoolean();
        static final AtomicBoolean sourceReleasedUnderWrite = new AtomicBoolean();
        static final AtomicReference<String> premiseFailure = new AtomicReference<>();
        static final AtomicInteger shardRequestsDispatched = new AtomicInteger();
        static final AtomicInteger firstShardRequestItemCount = new AtomicInteger();
        /// Captured while the bulk is running, because the client call does not hand one back and the cancel is addressed by task id.
        static final AtomicLong bulkTaskId = new AtomicLong(-1);

        static void reset() {
            writeParked = new CountDownLatch(1);
            releaseWrite = new CountDownLatch(1);
            writeReleased = new CountDownLatch(1);
            cancelLanded = new CountDownLatch(1);
            firstWriteSeen.set(false);
            sourceReleasedUnderWrite.set(false);
            premiseFailure.set(null);
            shardRequestsDispatched.set(0);
            firstShardRequestItemCount.set(0);
            bulkTaskId.set(-1);
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (INDEX.equals(indexModule.getIndex().getName())) {
                indexModule.addIndexOperationListener(new ParkFirstWrite());
            }
        }

        @Override
        public Collection<MappedActionFilter> getMappedActionFilters() {
            return List.of(new RecordBulkTask(), new HoldDispatchLoopUntilCancelled());
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

        /// Records the id of the task the test's bulk runs under, which is the task the test cancels.
        private static class RecordBulkTask implements MappedActionFilter {

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
                    && INDEX.equals(bulkRequest.requests().get(0).index())) {
                    bulkTaskId.compareAndSet(-1, task.getId());
                }
                chain.proceed(task, action, request, listener);
            }
        }

        /// Holds a primary write open while the bulk is cancelled, then records whether the document source it is about to index was
        /// freed underneath it.
        private static class ParkFirstWrite implements IndexingOperationListener {

            @Override
            public Engine.Index preIndex(ShardId shardId, Engine.Index index) {
                final BytesReference source = index.parsedDoc().source().originalBytes();
                // An update item can reach the engine with bytes the update path produced rather than the ones the caller owns, and
                // those say nothing about whether the caller's bytes outlived the write, so wait for one that still points at them
                if (index.origin() != Engine.Operation.Origin.PRIMARY
                    || source instanceof ReleasableBytesReference == false
                    || firstWriteSeen.compareAndSet(false, true) == false) {
                    return index;
                }
                try {
                    writeParked.countDown();
                    if (awaitQuietly(releaseWrite, PARKED_WRITE_TIMEOUT) == false) {
                        recordPremiseFailure("the parked write was never released");
                    } else {
                        // hasReferences() is the only accessor that stays safe to call once the bytes have been released
                        sourceReleasedUnderWrite.set(((ReleasableBytesReference) source).hasReferences() == false);
                    }
                } finally {
                    writeReleased.countDown();
                }
                return index;
            }
        }

        /// Runs inline on the thread driving `BulkOperation`'s dispatch loop, which makes it the only hook available between two shard
        /// dispatches of the same bulk. Once the first shard is writing, it holds the loop until the test has cancelled the bulk, so
        /// the next dispatch is rejected in [org.elasticsearch.tasks.TaskManager#registerChildConnection] by the ban that cancel put in
        /// place. A rejected dispatch never reaches this filter, which is how the dispatch count below tells the two shards apart.
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
                    && INDEX.equals(shardRequest.index())
                    && shardRequestsDispatched.incrementAndGet() == 1) {
                    firstShardRequestItemCount.set(shardRequest.items().length);
                    // TransportShardBulkAction registers its primary handler with the write pool, and
                    // TransportReplicationAction#handlePrimaryRequest dispatches to it before taking a permit, so this call returns
                    // once the operation is queued rather than once it is written. That is what frees this thread to hold the loop
                    // while the first shard is still writing.
                    chain.proceed(task, action, request, listener);
                    if (awaitQuietly(writeParked, SAFE_AWAIT_TIMEOUT) == false) {
                        recordPremiseFailure("the first shard request never reached the engine");
                    } else if (awaitQuietly(cancelLanded, SAFE_AWAIT_TIMEOUT) == false) {
                        recordPremiseFailure("the bulk was never cancelled while the dispatch loop was held");
                    }
                    return;
                }
                chain.proceed(task, action, request, listener);
            }
        }
    }

    /// [ReleasableBytesReference] that increments `releases` when it's released.
    private static ReleasableBytesReference pooled(BytesReference bytes, AtomicInteger releases) {
        return new ReleasableBytesReference(bytes, releases::incrementAndGet);
    }
}
