/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.plugins.NodeStatsPlugin;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Security plugin node stats. */
class DlsCacheStatistics implements NodeStatsPlugin.Statistics {

    public static final String WRITEABLE_NAME = "dls_cache_stats";

    private final Map<String, Object> dlsCacheStats;

    DlsCacheStatistics(Map<String, Object> dlsCacheStats) {
        // immutable copy, but also to preserve order in this::writeTo.
        // if passed-in map is Collections.unmodifiableMap(LinkedHashMap), it gets written as a HashMap with random order.
        this.dlsCacheStats = new LinkedHashMap<>(dlsCacheStats);
    }

    DlsCacheStatistics(StreamInput in) throws IOException {
        this.dlsCacheStats = in.readGenericMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(dlsCacheStats);
    }

    @Override
    public String getWriteableName() {
        return WRITEABLE_NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, Object> entry : dlsCacheStats.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        return o == this || (o instanceof DlsCacheStatistics that && Objects.equals(dlsCacheStats, that.dlsCacheStats));
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dlsCacheStats);
    }
}
