/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic;

import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model.NewRelicQueryResult;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.localserver.LocalServerTestBase;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.junit.Test;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NewRelicAdapterTest extends LocalServerTestBase {
    @Test
    public void testBrokerQuery() throws Exception {
        String query = NewRelicQuerySupplier.brokerQuery("TEST VALUE");

        this.serverBootstrap.registerHandler("*", new HttpRequestHandler() {
            @Override public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(HttpServletResponse.SC_OK);
                response.setEntity(brokerResponse());
            }
        });

        HttpHost httpHost = this.start();
        NewRelicAdapter adapter = new NewRelicAdapter(this.httpclient,
                "http://" + httpHost.getHostName() + ":" + httpHost.getPort(),
                0, "key");
        final List<NewRelicQueryResult> results = adapter.runQuery(query);

        assertEquals(results.size(), 2);

        NewRelicQueryResult firstResult = results.get(0);
        assertEquals(firstResult.getBrokerID(), 0);
        assertTrue(firstResult.getTopic() == null);
        assertEquals(firstResult.getPartition(), -1);
        Map<RawMetricType, Double> firstValues = firstResult.getResults();
        assertEquals(firstValues.get(BROKER_PRODUCE_LOCAL_TIME_MS_MEAN), 1.0, .1);
        assertEquals(firstValues.get(ALL_TOPIC_BYTES_IN), 2, .1);

        NewRelicQueryResult secondResult = results.get(1);
        assertEquals(secondResult.getBrokerID(), 8);
        assertTrue(secondResult.getTopic() == null);
        assertEquals(secondResult.getPartition(), -1);
        Map<RawMetricType, Double> secondValues = secondResult.getResults();
        assertEquals(secondValues.get(BROKER_PRODUCE_LOCAL_TIME_MS_MEAN), 2, .1);
        assertEquals(secondValues.get(ALL_TOPIC_BYTES_IN), 46207.055, .1);
    }

    private static HttpEntity brokerResponse() {
        return new StringEntity("{\"data\": {\"actor\": {\"account\": {\"nrql\": "
                + "{\"results\": [{\"max.produceLocalTimeMeanMs\": 1, \"max.bytesInPerSec\": 2, "
                + "\"facet\": \"0\", \"broker\": \"0\"}, {\"max.produceLocalTimeMeanMs\": 2, "
                + "\"max.bytesInPerSec\": 46207.055, \"facet\": \"8\", \"broker\": \"8\"}]}}}}}",
                StandardCharsets.UTF_8);
    }

    @Test
    public void testTopicQuery() throws Exception {
        String query = NewRelicQuerySupplier.topicQuery("TEST VALUE");

        this.serverBootstrap.registerHandler("*", new HttpRequestHandler() {
            @Override public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(HttpServletResponse.SC_OK);
                response.setEntity(topicResponse());
            }
        });

        HttpHost httpHost = this.start();
        NewRelicAdapter adapter = new NewRelicAdapter(this.httpclient,
                "http://" + httpHost.getHostName() + ":" + httpHost.getPort(),
                0, "key");
        final List<NewRelicQueryResult> results = adapter.runQuery(query);

        assertEquals(results.size(), 2);

        NewRelicQueryResult firstResult = results.get(0);
        assertEquals(firstResult.getBrokerID(), 1);
        assertTrue(firstResult.getTopic().equals("test_topic"));
        assertEquals(firstResult.getPartition(), -1);
        Map<RawMetricType, Double> firstValues = firstResult.getResults();
        assertEquals(firstValues.get(TOPIC_BYTES_IN), 1.0, .1);
        assertEquals(firstValues.get(TOPIC_BYTES_OUT), 2, .1);

        NewRelicQueryResult secondResult = results.get(1);
        assertEquals(secondResult.getBrokerID(), 2);
        assertTrue(secondResult.getTopic().equals("test_topic2"));
        assertEquals(secondResult.getPartition(), -1);
        Map<RawMetricType, Double> secondValues = secondResult.getResults();
        assertEquals(secondValues.get(TOPIC_BYTES_IN), 6, .1);
        assertEquals(secondValues.get(TOPIC_BYTES_OUT), 7, .1);
    }

    private static HttpEntity topicResponse() {
        return new StringEntity("{\"data\": {\"actor\": {\"account\": {\"nrql\": {\"results\": "
                + "[{\"facet\": [\"1\", \"test_topic\"], \"max.bytesInPerSec\": 1, \"max.bytesOutPerSec\": 2}, "
                + "{\"facet\": [\"2\", \"test_topic2\"], \"max.bytesInPerSec\": 6, \"max.bytesOutPerSec\": 7}]}}}}}",
                StandardCharsets.UTF_8);
    }

    @Test
    public void testPartitionQuery() throws Exception {
        String query = NewRelicQuerySupplier.partitionQuery("TestTopic");

        this.serverBootstrap.registerHandler("*", new HttpRequestHandler() {
            @Override public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(HttpServletResponse.SC_OK);
                response.setEntity(partitionResponse());
            }
        });

        HttpHost httpHost = this.start();
        NewRelicAdapter adapter = new NewRelicAdapter(this.httpclient,
                "http://" + httpHost.getHostName() + ":" + httpHost.getPort(),
                0, "key");
        final List<NewRelicQueryResult> results = adapter.runQuery(query);

        assertEquals(results.size(), 2);

        NewRelicQueryResult firstResult = results.get(0);
        assertEquals(firstResult.getBrokerID(), 6);
        assertTrue(firstResult.getTopic().equals("test_topic1"));
        assertEquals(firstResult.getPartition(), 0);
        Map<RawMetricType, Double> firstValues = firstResult.getResults();
        assertEquals(firstValues.get(PARTITION_SIZE), 262.0, .1);

        NewRelicQueryResult secondResult = results.get(1);
        assertEquals(secondResult.getBrokerID(), 5);
        assertTrue(secondResult.getTopic().equals("test_topic2"));
        assertEquals(secondResult.getPartition(), 14);
        Map<RawMetricType, Double> secondValues = secondResult.getResults();
        assertEquals(secondValues.get(PARTITION_SIZE), 135.0, .1);
    }

    private static HttpEntity partitionResponse() {
        return new StringEntity("{\"data\":{\"actor\":{\"account\":{\"nrql\":{\"results\":[{\"facet\":"
                + "[\"6\", \"test_topic1\", \"0\"], \"max.kafka_log_Log_Value_Size\":262.0}, "
                + "{\"facet\":[\"5\", \"test_topic2\", \"14\"], \"max.kafka_log_Log_Value_Size\""
                + ":135.0}]}}}}}", StandardCharsets.UTF_8);
    }

    @Test(expected = IOException.class)
    public void testFailureResponseWith403Code() throws Exception {
        String query = NewRelicQuerySupplier.brokerQuery("TestTopic");

        this.serverBootstrap.registerHandler("*", new HttpRequestHandler() {
            @Override public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(403);
                response.setEntity(brokerResponse());
            }
        });

        HttpHost httpHost = this.start();
        NewRelicAdapter adapter = new NewRelicAdapter(this.httpclient,
                "http://" + httpHost.getHostName() + ":" + httpHost.getPort(),
                0, "key");
        adapter.runQuery(query);
    }

    @Test(expected = IOException.class)
    public void testEmptyResponse() throws Exception {
        String query = NewRelicQuerySupplier.brokerQuery("TestTopic");

        this.serverBootstrap.registerHandler("*", new HttpRequestHandler() {
            @Override public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(HttpServletResponse.SC_OK);
                response.setEntity(new StringEntity(
                        "", StandardCharsets.UTF_8));
            }
        });

        HttpHost httpHost = this.start();
        NewRelicAdapter adapter = new NewRelicAdapter(this.httpclient,
                "http://" + httpHost.getHostName() + ":" + httpHost.getPort(),
                0, "key");
        adapter.runQuery(query);
    }

    @Test(expected = IOException.class)
    public void testInvalidJSONResponse() throws Exception {
        String query = NewRelicQuerySupplier.brokerQuery("TestTopic");

        this.serverBootstrap.registerHandler("*", new HttpRequestHandler() {
            @Override public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(HttpServletResponse.SC_OK);
                response.setEntity(new StringEntity(
                        "Invalid Input", StandardCharsets.UTF_8));
            }
        });

        HttpHost httpHost = this.start();
        NewRelicAdapter adapter = new NewRelicAdapter(this.httpclient,
                "http://" + httpHost.getHostName() + ":" + httpHost.getPort(),
                0, "key");
        adapter.runQuery(query);
    }

    // Note that we don't need to throw an exception here, but we just need to make sure no
    // new metrics are added which will be the case given that the size of results is 0
    @Test
    public void testEmptyResults() throws Exception {
        String query = NewRelicQuerySupplier.brokerQuery("TestTopic");

        this.serverBootstrap.registerHandler("*", new HttpRequestHandler() {
            @Override public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(HttpServletResponse.SC_OK);
                response.setEntity(new StringEntity("{\"data\":{\"actor\":{\"account\":{\"nrql\":"
                        + "{\"results\":[]}}}}}", StandardCharsets.UTF_8));
            }
        });

        HttpHost httpHost = this.start();
        NewRelicAdapter adapter = new NewRelicAdapter(this.httpclient,
                "http://" + httpHost.getHostName() + ":" + httpHost.getPort(),
                0, "key");
        final List<NewRelicQueryResult> results = adapter.runQuery(query);

        assertEquals(0, results.size());
    }
}
