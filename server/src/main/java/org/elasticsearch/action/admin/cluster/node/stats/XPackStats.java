/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;

/**
 * X-Pack and plugin contributed node statistics
 */
public class XPackStats implements Writeable, ChunkedToXContent {

    private final Map<String, NamedWriteable> extensionStats;

    public XPackStats(Map<String, NamedWriteable> extensionStats) {
        this.extensionStats = Objects.requireNonNull(extensionStats);
    }

    public XPackStats(StreamInput in) throws IOException {
        this.extensionStats = in.readMap(StreamInput::readString, in2 -> in2.readNamedWriteable(NamedWriteable.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(extensionStats, StreamOutput::writeString, StreamOutput::writeNamedWriteable);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return chunk((builder, p) -> {
            for (Map.Entry<String, NamedWriteable> entry : extensionStats.entrySet()) {
                if (entry.getValue() instanceof ToXContent toXContent) {
                    builder.field(entry.getKey(), toXContent, p);
                }
            }
            return builder;
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XPackStats that = (XPackStats) o;
        return Objects.equals(extensionStats, that.extensionStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(extensionStats);
    }
}
