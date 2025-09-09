/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xcontent.ToXContent;

import java.util.SequencedMap;
import java.util.function.Supplier;

/** An extension point for {@link Plugin} implementations that wish to contribute to node stats. */
public interface NodeStatsPlugin {

    /**
     * Extra node stats.
     * Key will be nested under each node in node stats response and needs to be unique across all plugins.
     * Keys and number of keys returned should always be the same, since they're registered on startup.
     */
    SequencedMap<String, Supplier<? extends Statistics>> getExtraNodeStats();

    /**
     * Extra statistics.
     * Can be of any serializable format. No restrictions. But do consider the amount of data returned.
     */
    interface Statistics extends ToXContent, NamedWriteable {}
}
