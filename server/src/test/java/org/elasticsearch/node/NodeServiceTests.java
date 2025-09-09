/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node;

import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.plugins.NodeStatsPlugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.support.AggregationUsageService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeServiceTests extends ESTestCase {
    public void testDuplicatePluginName() {
        final Settings settings = Settings.EMPTY;
        final ThreadPool threadPool = mock(ThreadPool.class);
        final MonitorService monitorService = mock(MonitorService.class);
        final Coordinator coordinator = mock(Coordinator.class);
        final TransportService transportService = mock(TransportService.class);
        final IndicesService indicesService = mock(IndicesService.class);
        final PluginsService pluginsService = mock(PluginsService.class);
        final CircuitBreakerService circuitBreakerService = mock(CircuitBreakerService.class);
        final ScriptService scriptService = mock(ScriptService.class);
        final HttpServerTransport httpServerTransport = mock(HttpServerTransport.class);
        final IngestService ingestService = mock(IngestService.class);
        final ClusterService clusterService = mock(ClusterService.class);
        final SettingsFilter settingsFilter = new SettingsFilter(List.of());
        final ResponseCollectorService responseCollectorService = new ResponseCollectorService(clusterService);
        final SearchTransportService searchTransportService = mock(SearchTransportService.class);
        final IndexingPressure indexingPressure = mock(IndexingPressure.class);
        final AggregationUsageService aggregationUsageService = mock(AggregationUsageService.class);
        final RepositoriesService repositoriesService = mock(RepositoriesService.class);
        final CompatibilityVersions compatibilityVersions = CompatibilityVersions.EMPTY;

        // Create mock plugins with the same name
        final NodeStatsPlugin plugin1 = mock(NodeStatsPlugin.class);
        final NodeStatsPlugin plugin2 = mock(NodeStatsPlugin.class);
        final NodeStatsPlugin.Statistics stats1 = mock(NodeStatsPlugin.Statistics.class);
        final NodeStatsPlugin.Statistics stats2 = mock(NodeStatsPlugin.Statistics.class);

        doReturn(List.of(stats1)).when(plugin1).getExtraNodeStats();
        doReturn(List.of(stats2)).when(plugin2).getExtraNodeStats();

        // Create tuples with duplicate plugin names
        final String duplicateName = "duplicate-plugin";
        final Stream<NodeStatsPlugin> pluginsWithDuplicateNames = Stream.of(plugin1, plugin2);

        when(pluginsService.filterPlugins(NodeStatsPlugin.class)).thenReturn(pluginsWithDuplicateNames);

        // Create NodeService instance
        final NodeService nodeService = new NodeService(
            settings,
            threadPool,
            monitorService,
            coordinator,
            transportService,
            indicesService,
            pluginsService,
            circuitBreakerService,
            scriptService,
            httpServerTransport,
            ingestService,
            clusterService,
            settingsFilter,
            responseCollectorService,
            searchTransportService,
            indexingPressure,
            aggregationUsageService,
            repositoriesService,
            compatibilityVersions
        );

        // Test that calling stats with pluginStats=true throws IllegalArgumentException
        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            nodeService.stats(
                new CommonStatsFlags(),
                false, // includeShardsStats
                false, // os
                false, // process
                false, // jvm
                false, // threadPool
                false, // fs
                false, // transport
                false, // http
                false, // circuitBreaker
                false, // script
                false, // discoveryStats
                false, // ingest
                false, // adaptiveSelection
                false, // scriptCache
                false, // indexingPressure
                false, // repositoriesStats
                true   // pluginStats - this should trigger the exception
            );
        });

        assertThat(exception.getMessage(), equalTo("Duplicate node stats plugin name: duplicate-plugin. Plugins: [duplicate-plugin, duplicate-plugin]"));
    }
}
