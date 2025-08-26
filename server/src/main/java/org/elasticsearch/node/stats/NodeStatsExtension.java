/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node.stats;

import org.elasticsearch.common.io.stream.NamedWriteable;

/**
 * Interface for components that want to contribute to node stats
 */
public interface NodeStatsExtension {
    /**
     * Returns the name of this extension, used as a field name in the response
     */
    String getName();

    /**
     * Collects stats for the current node
     * @return Statistics object to include in the response
     */
    NamedWriteable getStats();
}
