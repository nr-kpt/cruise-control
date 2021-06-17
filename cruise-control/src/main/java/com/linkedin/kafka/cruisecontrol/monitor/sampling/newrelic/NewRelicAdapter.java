/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model.NewRelicQueryResult;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;

class NewRelicAdapter {
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String GRAPHQL_API_PATH = "/graphql";

    private final CloseableHttpClient _httpClient;

    private final String _newRelicEndpoint;
    private final long _newRelicAccountId;
    private final String _newRelicApiKey;

    NewRelicAdapter(CloseableHttpClient httpClient, String newRelicEndpoint, int newRelicAccountId, String newRelicApiKey) {
        this._httpClient = validateNotNull(httpClient, "httpClient cannot be null");

        this._newRelicEndpoint = validateNotNull(newRelicEndpoint, "newRelicEndpoint cannot be null");
        this._newRelicAccountId = newRelicAccountId;
        this._newRelicApiKey = validateNotNull(newRelicApiKey, "newRelicApiKey cannot be null");
    }

    List<NewRelicQueryResult> runQuery(String query) throws IOException {
        URI uri = URI.create(_newRelicEndpoint + GRAPHQL_API_PATH);

        HttpPost httpGet = new HttpPost(uri);
        httpGet.addHeader("Content-Type", "application/json");
        httpGet.addHeader("API-Key", _newRelicApiKey);

        httpGet.setEntity(buildQueryPayload(query));

        try (CloseableHttpResponse response = _httpClient.execute(httpGet)) {
            int responseCode = response.getStatusLine().getStatusCode();

            HttpEntity entity = response.getEntity();
            InputStream content = entity.getContent();
            String responseBody = IOUtils.toString(content, StandardCharsets.UTF_8);

            if (responseCode != HttpServletResponse.SC_OK) {
                throw new IOException(String.format("Received non-success response code on New Relic GraphQL API HTTP call "
                        + "(response code = %s, response body = %s)", responseCode, responseBody));
            }

            JsonNode responseJson = JSON.readTree(responseBody);
            JsonNode resultsJson = responseJson.get("data").get("actor").get("account").get("nrql").get("results");

            List<NewRelicQueryResult> results = new ArrayList<>();

            // Can be null upon invalid query or just something else going wrong on serverside
            if (resultsJson == null) {
                return results;
            }

            int count = 0;
            for (JsonNode resultJson : resultsJson) {
                if (count == 0) {
                    System.out.printf("ResultJSON: %s%n", resultJson);
                }
                count++;
                results.add(new NewRelicQueryResult(resultJson));
            }

            return results;
        }
    }

    private HttpEntity buildQueryPayload(String query) throws JsonProcessingException {
        String payload = String.format(
                "{ actor { account(id: %s) { nrql(query: \"%s\") { results } } } }",
                _newRelicAccountId,
                query);

        ObjectNode payloadWrapper = JSON.createObjectNode();
        payloadWrapper.put("query", payload);
        payloadWrapper.put("variables", "");

        return new ByteArrayEntity(JSON.writeValueAsBytes(payloadWrapper));
    }
}
