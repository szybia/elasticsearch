/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.node.stats.NodeStatsExtension;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.DocumentSubsetBitsetCache;
import org.elasticsearch.xpack.security.authz.store.DlsCacheStats;

import java.io.IOException;
import java.util.Objects;

/**
 * Security extension for node stats
 */
public class SecurityNodeStatsExtension implements NodeStatsExtension {

    private static final String EXTENSION_NAME = "security";
    private final DocumentSubsetBitsetCache dlsBitsetCache;

    public SecurityNodeStatsExtension(DocumentSubsetBitsetCache dlsBitsetCache) {
        this.dlsBitsetCache = Objects.requireNonNull(dlsBitsetCache);
    }

    @Override
    public String getName() {
        return EXTENSION_NAME;
    }

    @Override
    public NamedWriteable getStats() {
        return new SecurityStats(new DlsCacheStats(dlsBitsetCache.usageStats()));
    }

    /**
     * Container for security-related stats that provides nested structure
     */
    public static class SecurityStats implements NamedWriteable, ToXContent {
        
        public static final String WRITEABLE_NAME = "security_stats";
        
        private final DlsCacheStats dlsCacheStats;
        
        public SecurityStats(DlsCacheStats dlsCacheStats) {
            this.dlsCacheStats = Objects.requireNonNull(dlsCacheStats);
        }
        
        public SecurityStats(StreamInput in) throws IOException {
            this.dlsCacheStats = new DlsCacheStats(in);
        }
        
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            dlsCacheStats.writeTo(out);
        }
        
        @Override
        public String getWriteableName() {
            return WRITEABLE_NAME;
        }
        
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("dls_cache");
            dlsCacheStats.toXContent(builder, params);
            builder.endObject();
            return builder;
        }
    }
}
