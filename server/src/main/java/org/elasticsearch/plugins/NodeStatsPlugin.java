/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.node.stats.NodeStatsExtension;

import java.util.List;

/**
 * An interface for plugins that wish to contribute to node stats
 */
public interface NodeStatsPlugin {
    /**
     * Return a collection of {@link NodeStatsExtension}s to contribute to node stats
     */
    List<NodeStatsExtension> getNodeStatsExtensions();
}
