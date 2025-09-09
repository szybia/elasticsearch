/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.plugins.NodeStatsPlugin;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;

/**
 * Extra plugin-contributed node statistics.
 */
public class PluginNodeStats implements Writeable, ChunkedToXContent {

    private final Map<String, ? extends NodeStatsPlugin.Statistics> pluginStatistics;

    public PluginNodeStats(Map<String, ? extends NodeStatsPlugin.Statistics> pluginStatistics) {
        this.pluginStatistics = new LinkedHashMap<>(pluginStatistics);
    }

    public PluginNodeStats(StreamInput in) throws IOException {
        this.pluginStatistics = in.readOrderedMap(StreamInput::readString, in2 -> in2.readNamedWriteable(NodeStatsPlugin.Statistics.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(pluginStatistics, StreamOutput::writeNamedWriteable);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return chunk((builder, p) -> {
            for (final var statistics : pluginStatistics.entrySet()) {
                builder.startObject(statistics.getKey());
                statistics.getValue().toXContent(builder, p);
                builder.endObject();
            }
            return builder;
        });
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof PluginNodeStats other && pluginStatistics.equals(other.pluginStatistics));
    }

    @Override
    public int hashCode() {
        return Objects.hash(pluginStatistics);
    }

    @Override
    public String toString() {
        return "PluginNodeStats{" + "statistics=" + pluginStatistics + '}';
    }
}
