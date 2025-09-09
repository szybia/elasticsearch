/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class PluggableNodeStatsIT extends SecurityIntegTestCase {

    @Override
    protected int maxNumberOfNodes() {
        return 2;
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // need real http
    }

    @SuppressWarnings("unchecked")
    public void testDlsCacheStats() throws Exception {
        final String dlsUser = "dls_user";
        final String dlsRole = "dls_role";
        final String indexName = "test_index";

        assertSecurityIndexActive();

        final var user = new User(dlsUser, dlsRole);
        getSecurityClient().putUser(user, new SecureString("password".toCharArray()));

        // Create a role with DLS
        final RoleDescriptor roleDescriptor = new RoleDescriptor(
            dlsRole,
            new String[] {},
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(indexName)
                    .privileges("read")
                    .allowRestrictedIndices(true)
                    .query("{\"term\": {\"public\": true}}")
                    .build()
            },
            null
        );
        getSecurityClient().putRole(roleDescriptor);

        // Get node names
        final String[] nodeNames = internalCluster().getNodeNames();
        final String nodeWithIndex = nodeNames[0];

        // Create an index on a specific node
        ensureGreen();
//        assertThat(
//            indicesAdmin()
//                .prepareCreate(indexName)
//                .setSettings(
//                    Settings.builder()
//                        .put("index.number_of_shards", 1)
//                        .put("index.number_of_replicas", 0)
//                        .put("index.routing.allocation.include._name", nodeWithIndex)
//                        .build()
//                )
//                .get().isAcknowledged(),
//            equalTo(true)
//        );

        // Index some documents
        client().prepareIndex(indexName).setId("1").setSource("public", true).get();
        client().prepareIndex(indexName).setId("2").setSource("public", false).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // Perform searches as the DLS user
        final Client dlsClient = client().filterWithHeader(
            Map.of("Authorization", UsernamePasswordToken.basicAuthHeaderValue(dlsUser, new SecureString("password".toCharArray())))
        );
        for (int i = 0; i < 5; i++) {
            SearchResponse response = dlsClient.prepareSearch(indexName).get();
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
        }

        // Get node stats
        final NodesStatsResponse statsResponse = client().admin()
            .cluster()
            .prepareNodesStats()
            .setPluginStats(true)
            .get();

        // Assert stats
        statsResponse.getNodes()
            .forEach(
                nodeStats -> {
                    assertThat(nodeStats.getPluginStats(), notNullValue());

                    final Map<String, Object> pluginStatsMap = toMap(nodeStats.getPluginStats());
                    assertThat(pluginStatsMap, hasKey("plugins"));
                    final Map<String, Object> plugins = (Map<String, Object>) pluginStatsMap.get("plugins");
                    assertThat(plugins, hasKey("security"));
                    final Map<String, Object> securityStats = (Map<String, Object>) plugins.get("security");
                    assertThat(securityStats, hasKey("dls_cache"));
                    final Map<String, Object> dlsCacheStats = (Map<String, Object>) securityStats.get("dls_cache");

                    if (nodeStats.getNode().getName().equals(nodeWithIndex)) {
                        assertThat(dlsCacheStats.get("hits"), instanceOf(Number.class));
                        assertThat(((Number) dlsCacheStats.get("hits")).longValue(), greaterThan(0L));
                        assertThat(dlsCacheStats.get("misses"), instanceOf(Number.class));
                        assertThat(((Number) dlsCacheStats.get("misses")).longValue(), greaterThan(0L));
                    } else {
                        assertThat(dlsCacheStats.get("hits"), equalTo(0));
                        assertThat(dlsCacheStats.get("misses"), equalTo(0));
                    }
                }
            );
    }

    private Map<String, Object> toMap(org.elasticsearch.action.admin.cluster.node.stats.PluginNodeStats stats) {
        try {
            XContentBuilder builder = jsonBuilder();
            builder.startObject();
            stats.toXContentChunked(ToXContent.EMPTY_PARAMS).forEachRemaining(x -> {
                try {
                    x.toXContent(builder, ToXContent.EMPTY_PARAMS);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            builder.endObject();
            return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
