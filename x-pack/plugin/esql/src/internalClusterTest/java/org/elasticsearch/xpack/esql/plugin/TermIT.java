/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.containsString;

public class TermIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        createAndPopulateIndex();
    }

    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request) {
        assumeTrue("term function capability not available", EsqlCapabilities.Cap.TERM_FUNCTION.isEnabled());
        return super.run(request);
    }

    public void testSimpleTermQuery() throws Exception {
        var query = """
            FROM test
            | WHERE term(content,"dog")
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(1), List.of(3), List.of(4), List.of(5)));
        }
    }

    public void testTermWithinEval() {
        var query = """
            FROM test
            | EVAL term_query = term(title,"fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[Term] function is only supported in WHERE and STATS commands"));
    }

    public void testMultipleTerm() {
        var query = """
            FROM test
            | WHERE term(content,"fox") AND term(content,"brown")
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(2), List.of(4), List.of(5)));
        }
    }

    public void testNotWhereTerm() {
        var query = """
            FROM test
            | WHERE NOT term(content,"brown")
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(3)));
        }
    }

    private void createAndPopulateIndex() {
        var indexName = "test";
        var client = client().admin().indices();
        var CreateRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("id", "type=integer", "content", "type=text");
        assertAcked(CreateRequest);
        client().prepareBulk()
            .add(
                new IndexRequest(indexName).id("1")
                    .source("id", 1, "content", "The quick brown animal swiftly jumps over a lazy dog", "title", "A Swift Fox's Journey")
            )
            .add(
                new IndexRequest(indexName).id("2")
                    .source("id", 2, "content", "A speedy brown fox hops effortlessly over a sluggish canine", "title", "The Fox's Leap")
            )
            .add(
                new IndexRequest(indexName).id("3")
                    .source("id", 3, "content", "Quick and nimble, the fox vaults over the lazy dog", "title", "Brown Fox in Action")
            )
            .add(
                new IndexRequest(indexName).id("4")
                    .source(
                        "id",
                        4,
                        "content",
                        "A fox that is quick and brown jumps over a dog that is quite lazy",
                        "title",
                        "Speedy Animals"
                    )
            )
            .add(
                new IndexRequest(indexName).id("5")
                    .source(
                        "id",
                        5,
                        "content",
                        "With agility, a quick brown fox bounds over a slow-moving dog",
                        "title",
                        "Foxes and Canines"
                    )
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow(indexName);
    }
}
