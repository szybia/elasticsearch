/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Statistics for Document Level Security (DLS) cache
 * Wraps the Map returned by DocumentSubsetBitsetCache.usageStats() directly
 */
public class DlsCacheStats implements NamedWriteable, ToXContent {

    public static final String WRITEABLE_NAME = "dls_cache_stats";

    private final Map<String, Object> stats;

    public DlsCacheStats(Map<String, Object> stats) {
        this.stats = Objects.requireNonNull(stats);
    }

    public DlsCacheStats(StreamInput in) throws IOException {
        this(in.readGenericMap());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(stats);
    }

    @Override
    public String getWriteableName() {
        return WRITEABLE_NAME;
    }

    public Map<String, Object> getStats() {
        return stats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.map(stats);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (
            o instanceof DlsCacheStats that &&
            Objects.equals(stats, that.stats)
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(stats);
    }

    @Override
    public String toString() {
        return "DlsCacheStats{" + stats + '}';
    }
}
