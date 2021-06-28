/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic;

import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSamplerOptions;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model.NewRelicQueryResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.MetricScope.BROKER;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.MetricScope.TOPIC;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.NewRelicMetricSampler.*;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for NewRelicMetricSampler.
 */
public class NewRelicMetricSamplerTest {
    private static final double DOUBLE_DELTA = 0.00000001;
    private static final double BYTES_IN_KB = 1024.0;

    private static final int FIXED_VALUE = 94;
    private static final long START_EPOCH_SECONDS = 1603301400L;
    private static final long START_TIME_MS = TimeUnit.SECONDS.toMillis(START_EPOCH_SECONDS);
    private static final long END_TIME_MS = START_TIME_MS + TimeUnit.SECONDS.toMillis(59);

    private static final int TOTAL_BROKERS = 3;
    private static final int TOTAL_PARTITIONS = 3;

    private static final String TEST_TOPIC1 = "test-topic1";
    private static final String TEST_TOPIC2 = "test-topic2";
    private static final String TEST_TOPIC3 = "test-topic3";

    private NewRelicMetricSampler _newRelicMetricSampler;
    private NewRelicAdapter _newRelicAdapter;
    private Map<RawMetricType.MetricScope, String> _queryMap;

    /**
     * Set up mocks
     */
    @Before
    public void setUp() {
        _newRelicAdapter = mock(NewRelicAdapter.class);
        _newRelicMetricSampler = new NewRelicMetricSampler();
        _queryMap = new NewRelicQuerySupplier().get();
    }

    @Test(expected = ConfigException.class)
    public void testNoEndpointProvided() throws Exception {
        Map<String, Object> config = new HashMap<>();
        addCapacityConfig(config);
        config.put(NEWRELIC_API_KEY_CONFIG, "ABC");
        config.put(NEWRELIC_ACCOUNT_ID_CONFIG, 1);
        config.put(NEWRELIC_QUERY_LIMIT_CONFIG, 10);
        _newRelicMetricSampler.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void testNoAPIKeyProvided() throws Exception {
        Map<String, Object> config = new HashMap<>();
        addCapacityConfig(config);
        config.put(NEWRELIC_ENDPOINT_CONFIG, "https://staging-api.newrelic.com");
        config.put(NEWRELIC_ACCOUNT_ID_CONFIG, 1);
        config.put(NEWRELIC_QUERY_LIMIT_CONFIG, 10);
        _newRelicMetricSampler.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void testNoAccountIDProvided() throws Exception {
        Map<String, Object> config = new HashMap<>();
        addCapacityConfig(config);
        config.put(NEWRELIC_ENDPOINT_CONFIG, "https://staging-api.newrelic.com");
        config.put(NEWRELIC_API_KEY_CONFIG, "ABC");
        config.put(NEWRELIC_QUERY_LIMIT_CONFIG, 10);
        _newRelicMetricSampler.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void testNoQueryLimitProvided() throws Exception {
        Map<String, Object> config = new HashMap<>();
        addCapacityConfig(config);
        config.put(NEWRELIC_ENDPOINT_CONFIG, "https://staging-api.newrelic.com");
        config.put(NEWRELIC_API_KEY_CONFIG, "ABC");
        config.put(NEWRELIC_ACCOUNT_ID_CONFIG, 1);
        _newRelicMetricSampler.configure(config);
    }

    @Test
    public void testGetSamplesSuccess() throws Exception {
        Map<String, Object> config = new HashMap<>();
        setConfigs(config, 14);
        addCapacityConfig(config);

        ArrayList<String> topics = new ArrayList<>();
        topics.add(TEST_TOPIC1);
        topics.add(TEST_TOPIC2);

        ArrayList<Integer> partitions = new ArrayList<>();
        partitions.add(3);
        partitions.add(5);

        setUp();
        MetricSamplerOptions metricSamplerOptions = buildMetricSamplerOptions(topics, partitions);
        _newRelicMetricSampler.configure(config);
        _newRelicMetricSampler._newRelicAdapter = _newRelicAdapter;

        setupNewRelicAdapterMock(buildBrokerResults(),
                buildTopicResults(topics), buildPartitionResults(topics, partitions));

        replay(_newRelicAdapter);
        //MetricSampler.Samples samples = _newRelicMetricSampler.getSamples(metricSamplerOptions);
        _newRelicMetricSampler.getSamples(metricSamplerOptions);

        //assertSamplesValid(samples, topics);
        verify(_newRelicAdapter);
    }

    private void assertSamplesValid(MetricSampler.Samples samples, ArrayList<String> topics) {
        assertEquals(TOTAL_BROKERS, samples.brokerMetricSamples().size());
        assertEquals(topics.size(), 2);
    }

    @Test
    public void testPartitionQueriesWithRandomInputs() throws Exception {
        Map<String, Object> config = new HashMap<>();
        setConfigs(config);
        addCapacityConfig(config);
    }

    private static MetricSamplerOptions buildMetricSamplerOptions(ArrayList<String> topics, ArrayList<Integer> partitions) {

        return new MetricSamplerOptions(
                generateCluster(topics, partitions),
                generatePartitions(topics, partitions),
                START_TIME_MS,
                END_TIME_MS,
                MetricSampler.SamplingMode.ALL,
                KafkaMetricDef.commonMetricDef(),
                60000
        );
    }

    private void setConfigs(Map<String, Object> config) {
        config.put(NEWRELIC_ENDPOINT_CONFIG, "https://staging-api.newrelic.com");
        config.put(NEWRELIC_API_KEY_CONFIG, "ABC");
        config.put(NEWRELIC_ACCOUNT_ID_CONFIG, 1);
        config.put(NEWRELIC_QUERY_LIMIT_CONFIG, 10);
    }

    private void setConfigs(Map<String, Object> config, int queryLimit) {
        config.put(NEWRELIC_ENDPOINT_CONFIG, "https://staging-api.newrelic.com");
        config.put(NEWRELIC_API_KEY_CONFIG, "ABC");
        config.put(NEWRELIC_ACCOUNT_ID_CONFIG, 1);
        config.put(NEWRELIC_QUERY_LIMIT_CONFIG, queryLimit);
    }

    private void setupNewRelicAdapterMock(List<NewRelicQueryResult> brokerResults,
                                          List<NewRelicQueryResult> topicResults,
                                          List<NewRelicQueryResult> partitionResults) throws IOException {
            expect(_newRelicAdapter.runQuery(eq(new NewRelicQuerySupplier().get().get(BROKER))))
                    .andReturn(brokerResults);
            expect(_newRelicAdapter.runQuery(eq(new NewRelicQuerySupplier().get().get(TOPIC))))
                    .andReturn(topicResults);
            expect(_newRelicAdapter.runQuery(contains("FROM Metric")))
                    .andReturn(partitionResults).anyTimes();
    }

    private static List<NewRelicQueryResult> buildBrokerResults() {
        List<NewRelicQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < TOTAL_BROKERS; brokerId++) {
            Map<RawMetricType, Double> results = new HashMap<>();
            // FIXME -> put values into results
            resultList.add(new NewRelicQueryResult(brokerId, results));
        }
        return resultList;
    }

    private static List<NewRelicQueryResult> buildTopicResults(ArrayList<String> topics) {
        List<NewRelicQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < TOTAL_BROKERS; brokerId++) {
            // FIXME
        }
        return resultList;
    }

    private static List<NewRelicQueryResult> buildPartitionResults(ArrayList<String> topics,
                                                                   ArrayList<Integer> partitions) {
        List<NewRelicQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < TOTAL_BROKERS; brokerId++) {
            // FIXME
        }
        return resultList;
    }

    private void addCapacityConfig(Map<String, Object> config) throws IOException {
        File capacityConfigFile = File.createTempFile("capacityConfig", "json");
        FileOutputStream fileOutputStream = new FileOutputStream(capacityConfigFile);
        try (OutputStreamWriter writer = new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8)) {
            writer.write("{\n"
                    + "  \"brokerCapacities\":[\n"
                    + "    {\n"
                    + "      \"brokerId\": \"-1\",\n"
                    + "      \"capacity\": {\n"
                    + "        \"DISK\": \"100000\",\n"
                    + "        \"CPU\": {\"num.cores\": \"4\"},\n"
                    + "        \"NW_IN\": \"5000000\",\n"
                    + "        \"NW_OUT\": \"5000000\"\n"
                    + "      }\n"
                    + "    }\n"
                    + "  ]\n"
                    + "}\n");
        }
        config.put("capacity.config.file", capacityConfigFile.getAbsolutePath());
        BrokerCapacityConfigResolver brokerCapacityConfigResolver = new BrokerCapacityConfigFileResolver();
        config.put("broker.capacity.config.resolver.object", brokerCapacityConfigResolver);
        config.put("sampling.allow.cpu.capacity.estimation", true);
        brokerCapacityConfigResolver.configure(config);
    }

    private static Set<TopicPartition> generatePartitions(ArrayList<String> topics, ArrayList<Integer> partitions) {
        Set<TopicPartition> set = new HashSet<>();
        // For each topic add the same number of partitions
        for (int i = 0; i < topics.size(); i++) {
            for (int partition = 0; partition < partitions.get(i); partition++) {
                TopicPartition topicPartition = new TopicPartition(topics.get(i), partition);
                set.add(topicPartition);
            }
        }
        return set;
    }

    private static Cluster generateCluster(ArrayList<String> topics, ArrayList<Integer> partitions) {
        Node[] allNodes = new Node[TOTAL_BROKERS];
        Set<PartitionInfo> partitionInfo = new HashSet<>(TOTAL_BROKERS);
        for (int brokerId = 0; brokerId < TOTAL_BROKERS; brokerId++) {
            allNodes[brokerId] = new Node(brokerId, "broker-" + brokerId + ".test-cluster.org", 9092);
        }
        for (int i = 0; i < topics.size(); i++) {
            for (int partitionId = 0; partitionId < partitions.get(i); partitionId++) {
                partitionInfo.add(new PartitionInfo(topics.get(i), partitionId,
                        allNodes[partitionId % TOTAL_BROKERS], allNodes, allNodes));
            }
        }
        return new Cluster("cluster_id", Arrays.asList(allNodes),
                partitionInfo, Collections.emptySet(), Collections.emptySet());
    }
}
