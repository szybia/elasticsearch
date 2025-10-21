/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import java.util.List;

/**
 * Listener invoked after index settings have been applied within the same master task
 * in order to perform follow-up logic based on specific settings changes.
 */
public interface IndexSettingsAppliedListener {

    /**
     * Notifies that the lifecycle policy setting (index.lifecycle.name) for one or more indices has changed.
     * Implementations may mutate the provided {@link ProjectMetadata.Builder}. The return value should be true
     * if the builder was modified, false otherwise.
     */
    boolean onLifecycleNameChanged(
        ProjectId projectId,
        ProjectMetadata currentProject,
        ProjectMetadata.Builder builder,
        List<String> indices,
        String newLifecyclePolicy
    );
}


