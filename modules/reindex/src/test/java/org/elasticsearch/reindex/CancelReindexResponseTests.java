/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class CancelReindexResponseTests extends AbstractWireSerializingTestCase<CancelReindexResponse> {

    @BeforeClass
    public static void beforeClass() {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
    }

    public void testToXContentAcknowledgedOnly() throws IOException {
        Map<String, Object> asMap = XContentTestUtils.convertToMap(createTestInstance());
        assertEquals(Map.of("acknowledged", true), asMap);
    }

    @Override
    protected CancelReindexResponse createTestInstance() {
        return new CancelReindexResponse(List.of(), List.of());
    }

    @Override
    protected CancelReindexResponse mutateInstance(CancelReindexResponse r) {
        if (randomBoolean()) {
            return new CancelReindexResponse(
                generateNumOfTaskOperationFailures(r.getTaskFailures().size() + 1),
                toFailedNodeExceptionList(r.getNodeFailures())
            );
        } else {
            return new CancelReindexResponse(r.getTaskFailures(), randomNumOfFailedNodeExceptionsOtherThan(r.getNodeFailures().size() + 1));
        }
    }

    @Override
    protected Writeable.Reader<CancelReindexResponse> instanceReader() {
        return CancelReindexResponse::new;
    }

    private TaskOperationFailure randomTaskOperationFailure() {
        return new TaskOperationFailure(
            randomAlphaOfLength(5),
            randomLongBetween(1, 1_000_000),
            new ElasticsearchException("task failure")
        );
    }

    private List<TaskOperationFailure> generateNumOfTaskOperationFailures(int thisNumberOfFailures) {
        return IntStream.rangeClosed(thisNumberOfFailures, thisNumberOfFailures).mapToObj(i -> randomTaskOperationFailure()).toList();
    }

    private List<FailedNodeException> randomNumOfFailedNodeExceptionsOtherThan(int thisNumberOfFailures) {
        return IntStream.rangeClosed(thisNumberOfFailures, thisNumberOfFailures).mapToObj(i -> randomFailedNodeException()).toList();
    }

    private FailedNodeException randomFailedNodeException() {
        return new FailedNodeException(randomAlphaOfLength(10), "node failure", new ElasticsearchException("node failure"));
    }

    private static List<FailedNodeException> toFailedNodeExceptionList(List<? extends ElasticsearchException> exceptions) {
        return exceptions.stream().map(e -> (FailedNodeException) e).toList();
    }
}
