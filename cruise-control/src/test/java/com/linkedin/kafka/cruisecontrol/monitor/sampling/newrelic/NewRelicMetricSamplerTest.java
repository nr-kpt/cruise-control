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

    /**
     * Set up mocks
     */
    @Before
    public void setUp() {
        _newRelicAdapter = mock(NewRelicAdapter.class);
        _newRelicMetricSampler = new NewRelicMetricSampler();
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
        MetricSamplerOptions metricSamplerOptions = buildMetricSamplerOptions(TOTAL_BROKERS, topics, partitions);
        _newRelicMetricSampler.configure(config);
        _newRelicMetricSampler._newRelicAdapter = _newRelicAdapter;

        setupNewRelicAdapterMock(buildBrokerResults(TOTAL_BROKERS),
                buildTopicResults(TOTAL_BROKERS, topics), buildPartitionResults(TOTAL_BROKERS, topics, partitions));

        replay(_newRelicAdapter);
        MetricSampler.Samples samples = _newRelicMetricSampler.getSamples(metricSamplerOptions);

        assertSamplesValid(samples, topics);
        verify(_newRelicAdapter);
    }

    // Constructing NRQL queries correctly
    // Constructing correct number of NRQL queries

    @Test
    public void testTooManyReplicas() throws Exception {
        Map<String, Object> config = new HashMap<>();
        setConfigs(config, 2);
        addCapacityConfig(config);

        ArrayList<String> topics = new ArrayList<>();
        topics.add(TEST_TOPIC1);

        ArrayList<Integer> partitions = new ArrayList<>();
        partitions.add(3);

        setUp();
        MetricSamplerOptions metricSamplerOptions = buildMetricSamplerOptions(TOTAL_BROKERS, topics, partitions);
        _newRelicMetricSampler.configure(config);
        _newRelicMetricSampler._newRelicAdapter = _newRelicAdapter;

        setupNewRelicAdapterMock(buildBrokerResults(TOTAL_BROKERS),
                buildTopicResults(TOTAL_BROKERS, topics), buildPartitionResults(TOTAL_BROKERS, topics, partitions));

        replay(_newRelicAdapter);
        MetricSampler.Samples samples = _newRelicMetricSampler.getSamples(metricSamplerOptions);

        assertSamplesValid(samples, topics);
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

    private static MetricSamplerOptions buildMetricSamplerOptions(int numBrokers,
                                                                  ArrayList<String> topics,
                                                                  ArrayList<Integer> partitions) {

        return new MetricSamplerOptions(
                generateCluster(numBrokers, topics, partitions),
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
            expect(_newRelicAdapter.runQuery(eq(NewRelicQuerySupplier.brokerQuery())))
                    .andReturn(brokerResults);
            expect(_newRelicAdapter.runQuery(startsWith("FROM KafkaBrokerTopicStats SELECT max(messagesInPerSec), "
                    + "max(bytesInPerSec), max(bytesOutPerSec), max(totalProduceRequestsPerSec), "
                    + "max(totalFetchRequestsPerSec) WHERE cluster = 'test-odd-wire-kafka'")))
                    .andReturn(topicResults).anyTimes();

            String beforePattern = "FROM Metric SELECT max\\(kafka_log_Log_Value_Size\\) "
                    + "WHERE entity.name = '.*' WHERE ";
            String afterPattern = " FACET broker, topic, partition SINCE 1 minute ago LIMIT MAX";
            String partitionMatcher = beforePattern + ".*" + afterPattern;
            expect(_newRelicAdapter.runQuery(matches(partitionMatcher)))
                    .andReturn(partitionResults).anyTimes();
    }

    private static List<NewRelicQueryResult> buildBrokerResults(int numBrokers) {
        List<NewRelicQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
            Map<RawMetricType, Double> results = new HashMap<>();
            for (RawMetricType type: RawMetricType.allMetricTypes()) {
                if (type.metricScope() == BROKER) {
                    results.put(type, Math.random());
                }
            }
            resultList.add(new NewRelicQueryResult(brokerId, results));
        }
        return resultList;
    }

    private static List<NewRelicQueryResult> buildTopicResults(int numBrokers, ArrayList<String> topics) {
        List<NewRelicQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
            for (String topic: topics) {
                Map<RawMetricType, Double> results = new HashMap<>();
                for (RawMetricType type: RawMetricType.topicMetricTypes()) {
                    results.put(type, Math.random());
                }
                resultList.add(new NewRelicQueryResult(brokerId, topic, results));
            }
        }
        return resultList;
    }

    private static List<NewRelicQueryResult> buildPartitionResults(int numBrokers, ArrayList<String> topics,
                                                                   ArrayList<Integer> partitions) {
        List<NewRelicQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
            for (int i = 0; i < topics.size(); i++) {
                for (int partition = 0; partition < partitions.get(i); partition++) {
                    Map<RawMetricType, Double> results = new HashMap<>();
                    for (RawMetricType type : RawMetricType.partitionMetricTypes()) {
                        results.put(type, Math.random());
                    }
                    resultList.add(new NewRelicQueryResult(brokerId, topics.get(i), partition, results));
                }
            }
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
        for (int i = 0; i < topics.size(); i++) {
            for (int partition = 0; partition < partitions.get(i); partition++) {
                TopicPartition topicPartition = new TopicPartition(topics.get(i), partition);
                set.add(topicPartition);
            }
        }
        return set;
    }

    private static Cluster generateCluster(int numBrokers, ArrayList<String> topics, ArrayList<Integer> partitions) {
        Node[] allNodes = new Node[numBrokers];
        Set<PartitionInfo> partitionInfo = new HashSet<>(numBrokers);
        for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
            allNodes[brokerId] = new Node(brokerId, "broker-" + brokerId + ".test-cluster.org", 9092);
        }
        for (int i = 0; i < topics.size(); i++) {
            for (int partitionId = 0; partitionId < partitions.get(i); partitionId++) {
                partitionInfo.add(new PartitionInfo(topics.get(i), partitionId,
                        allNodes[partitionId % numBrokers], allNodes, allNodes));
            }
        }
        return new Cluster("cluster_id", Arrays.asList(allNodes),
                partitionInfo, Collections.emptySet(), Collections.emptySet());
    }
}
