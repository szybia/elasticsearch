/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.plugins.NodeStatsPlugin;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;

public class PluginNodeStatsTests extends AbstractWireSerializingTestCase<PluginNodeStats> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(List.of(
            new NamedWriteableRegistry.Entry(NodeStatsPlugin.Statistics.class, TestNodeStatsPluginStats.WRITEABLE_NAME, TestNodeStatsPluginStats::new)
        ));
    }

    @Override
    protected PluginNodeStats createTestInstance() {
        final Map<String, NodeStatsPlugin.Statistics> stats = new LinkedHashMap<>();
        final int numStats = randomIntBetween(0, 3);
        for (int i = 0; i < numStats; i++) {
            stats.put("stat-" + i, new TestNodeStatsPluginStats("value-" + randomAlphaOfLength(5)));
        }
        return new PluginNodeStats(stats);
    }

    @Override
    protected PluginNodeStats mutateInstance(PluginNodeStats instance) {
        return new PluginNodeStats(Map.of("mutated-stat", new TestNodeStatsPluginStats("mutated-value")));
    }

    @Override
    protected Writeable.Reader<PluginNodeStats> instanceReader() {
        return PluginNodeStats::new;
    }

    public void testOrderAndSerializationAndDeserialization() throws IOException {
        final Map<String, NodeStatsPlugin.Statistics> stats = new LinkedHashMap<>();
        stats.put("first", new TestNodeStatsPluginStats("first-value"));
        stats.put("second", new TestNodeStatsPluginStats("second-value"));
        stats.put("third", new TestNodeStatsPluginStats("third-value"));

        final PluginNodeStats original = new PluginNodeStats(stats);
        final PluginNodeStats deserialized = copyInstance(original);

        try (final var builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            deserialized.toXContentChunked(EMPTY_PARAMS).forEachRemaining(xContent -> {
                try {
                    xContent.toXContent(builder, EMPTY_PARAMS);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });
            builder.endObject();

            final String json = XContentHelper.convertToJson(BytesReference.bytes(builder), false, builder.contentType());
            assertThat(json, equalTo("""
                {"first":{"key":"first-value"},"second":{"key":"second-value"},"third":{"key":"third-value"}}"""
            ));
        }
    }
}
