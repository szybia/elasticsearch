/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CancelReindexRequestTests extends AbstractWireSerializingTestCase<CancelReindexRequest> {

    @BeforeClass
    public static void beforeClass() {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
    }

    public void testValidate() {
        CancelReindexRequest request = new CancelReindexRequest(randomProjectIdOrDefault(), randomBoolean());
        assertThat(request.validate().validationErrors(), equalTo(List.of("task id must be provided")));
    }

    public void testMatchThrowsUnsupportedOperation() {
        CancelReindexRequest request = new CancelReindexRequest(randomProjectIdOrDefault(), randomBoolean());
        final var e = assertThrows(UnsupportedOperationException.class, () -> request.match(null));
        assertThat(e.getMessage(), is("shouldn't be called. transport overrides function which does."));
    }

    @Override
    protected Writeable.Reader<CancelReindexRequest> instanceReader() {
        return CancelReindexRequest::new;
    }

    @Override
    protected CancelReindexRequest createTestInstance() {
        CancelReindexRequest request = new CancelReindexRequest(randomProjectIdOrDefault(), randomBoolean());
        request.setTargetTaskId(randomTaskId());
        request.setTimeout(randomTimeValue());
        return request;
    }

    @Override
    protected CancelReindexRequest mutateInstance(CancelReindexRequest r) {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> copyRequest(randomValueOtherThan(r.getProjectId(), ESTestCase::randomProjectIdOrDefault), r.waitForCompletion(), r);
            case 1 -> copyRequest(r.getProjectId(), r.waitForCompletion() == false, r);
            case 2 -> {
                CancelReindexRequest request = copyRequest(r.getProjectId(), r.waitForCompletion(), r);
                request.setTargetTaskId(randomValueOtherThan(r.getTargetTaskId(), CancelReindexRequestTests::randomTaskId));
                yield request;
            }
            case 3 -> {
                CancelReindexRequest request = copyRequest(r.getProjectId(), r.waitForCompletion(), r);
                request.setTimeout(randomValueOtherThan(r.getTimeout(), ESTestCase::randomTimeValue));
                yield request;
            }
            default -> throw new IllegalStateException("unexpected mutation branch");
        };
    }

    private CancelReindexRequest copyRequest(ProjectId projectId, boolean waitForCompletion, CancelReindexRequest original) {
        CancelReindexRequest request = new CancelReindexRequest(projectId, waitForCompletion);
        request.setTargetTaskId(original.getTargetTaskId());
        request.setTimeout(original.getTimeout());
        return request;
    }

    private static TaskId randomTaskId() {
        return new TaskId(randomAlphaOfLengthBetween(3, 8), randomNonNegativeLong());
    }
}
