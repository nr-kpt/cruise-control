/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic;

import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.kafka.cruisecontrol.exception.SamplingException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.BrokerMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.PartitionMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.TopicMetric;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.AbstractMetricSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSamplerOptions;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model.NewRelicQueryResult;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.Collections;
import java.util.Objects;
import java.util.HashMap;


public class NewRelicMetricSampler extends AbstractMetricSampler {
    private static final Logger LOGGER = LoggerFactory.getLogger(NewRelicMetricSampler.class);

    // Config name visible to tests
    static final String NEWRELIC_ENDPOINT_CONFIG = "newrelic.endpoint";
    static final String NEWRELIC_API_KEY_CONFIG = "newrelic.api.key";
    static final String NEWRELIC_ACCOUNT_ID_CONFIG = "newrelic.account.id";
    static final String NEWRELIC_QUERY_LIMIT_CONFIG = "newrelic.query.limit";

    // We make this protected so we can set it during the tests
    protected NewRelicAdapter _newRelicAdapter;
    private CloseableHttpClient _httpClient;

    // NRQL Query limit
    private static int MAX_SIZE;

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        configureNewRelicAdapter(configs);
        configureQueries(configs);
    }

    private void configureQueries(Map<String, ?> configs) {
        if (!configs.containsKey(NEWRELIC_QUERY_LIMIT_CONFIG)) {
            throw new ConfigException(String.format(
                    "%s config is required to have a query limit", NEWRELIC_QUERY_LIMIT_CONFIG));
        }
        MAX_SIZE = (Integer) configs.get(NEWRELIC_QUERY_LIMIT_CONFIG);
    }

    private void configureNewRelicAdapter(Map<String, ?> configs) {
        final String endpoint = (String) configs.get(NEWRELIC_ENDPOINT_CONFIG);
        if (endpoint == null) {
            throw new ConfigException(String.format(
                    "%s config is required to have an endpoint", NEWRELIC_API_KEY_CONFIG));
        }

        final String apiKey = (String) configs.get(NEWRELIC_API_KEY_CONFIG);
        if (apiKey == null) {
            throw new ConfigException(String.format(
                    "%s config is required to have an API Key", NEWRELIC_API_KEY_CONFIG));
        }
        if (!configs.containsKey(NEWRELIC_ACCOUNT_ID_CONFIG)) {
            throw new ConfigException(String.format(
                    "%s config is required to have an account ID", NEWRELIC_ACCOUNT_ID_CONFIG));
        }
        final int accountId = (Integer) configs.get(NEWRELIC_ACCOUNT_ID_CONFIG);

        _httpClient = HttpClients.createDefault();
        _newRelicAdapter = new NewRelicAdapter(_httpClient, endpoint, accountId, apiKey);

    }

    // This function will run all our queries using NewRelicAdapter
    // Note that under the current implementation of this class, we assume that no topic will have
    // more than MAX_SIZE number of replicas in any one broker
    // We also assume no cluster has more than MAX_SIZE brokers
    @Override
    protected int retrieveMetricsForProcessing(MetricSamplerOptions metricSamplerOptions) throws SamplingException {
        // FIXME
        int[] resultCounts = new int[]{0, 0};

        // Run our broker level queries
        runBrokerQuery(resultCounts);

        // Run topic level queries
        runTopicQueries(metricSamplerOptions.cluster(), resultCounts);

        // Run partition level queries
        runPartitionQueries(metricSamplerOptions.cluster(), resultCounts);

        LOGGER.info("Added {} metric values. Skipped {} invalid query results.", resultCounts[0], resultCounts[1]);
        return resultCounts[0];
    }

    private void runBrokerQuery(int[] resultCounts) throws SamplingException {
        // Run our broker query first
        final String brokerQuery = NewRelicQuerySupplier.brokerQuery();
        final List<NewRelicQueryResult> brokerResults;

        try {
            brokerResults = _newRelicAdapter.runQuery(brokerQuery);
        } catch (IOException e) {
            LOGGER.error("Error when attempting to query NRQL for metrics.", e);
            //throw new SamplingException("Could not query metrics from NRQL.");
            return;
        }

        for (NewRelicQueryResult result : brokerResults) {
            try {
                resultCounts[0] += addBrokerMetrics(result);
            } catch (InvalidNewRelicResultException e) {
                // Unlike PrometheusMetricSampler, this form of exception is probably very unlikely since
                // we will be getting cleaned up and well formed data directly from NRDB, but just keeping
                // this check here anyway to be safe
                LOGGER.trace("Invalid query result received from New Relic for query {}", brokerQuery, e);
                resultCounts[1]++;
            }
        }
    }

    private void runTopicQueries(Cluster cluster, int[] resultCounts) {
        // Get the sorted list of brokers by their topic counts
        List<KafkaSize> brokerSizes = getSortedBrokersByTopicCount(cluster);

        List<NewRelicQueryBin> brokerQueryBins;
        try {
            brokerQueryBins = assignToBins(brokerSizes, BrokerQueryBin.class);

            // Generate the queries based on the bins that PartitionCounts were assigned to
            List<String> topicQueries = getTopicQueries(brokerQueryBins);

            // Run the partition queries
            for (String query: topicQueries) {
                final List<NewRelicQueryResult> queryResults;

                try {
                    System.out.printf("Topic Query: %s%n", query);
                    queryResults = _newRelicAdapter.runQuery(query);
                } catch (IOException e) {
                    LOGGER.error("Error when attempting to query NRQL for metrics.", e);
                    //throw new SamplingException("Could not query metrics from NRQL.");
                    continue;
                }

                for (NewRelicQueryResult result : queryResults) {
                    try {
                        resultCounts[0] += addTopicMetrics(result);
                    } catch (InvalidNewRelicResultException e) {
                        // Unlike PrometheusMetricSampler, this form of exception is probably very unlikely since
                        // we will be getting cleaned up and well formed data directly from NRDB, but just keeping
                        // this check here anyway to be safe
                        LOGGER.trace("Invalid query result received from New Relic for topic query {}", query, e);
                        resultCounts[1]++;
                    }
                }
            }

        } catch (Exception e) {
            LOGGER.error("Error when converting topics to bins.", e);
        }
    }

    // Remaining tasks -> use futures in queries +

    private void runPartitionQueries(Cluster cluster, int[] resultCounts) {
        // Get the sorted list of topics by their leader + follower count for each partition
        List<KafkaSize> topicSizes = getSortedTopicsByReplicaCount(cluster);

        // Use FFD algorithm (more info at method header) to assign topicSizes to queries
        List<NewRelicQueryBin> topicQueryBins;
        try {
            topicQueryBins = assignToBins(topicSizes, TopicQueryBin.class);

            // Generate the queries based on the bins that PartitionCounts were assigned to
            List<String> partitionQueries = getPartitionQueries(topicQueryBins);

            // Run the partition queries
            for (String query: partitionQueries) {
                final List<NewRelicQueryResult> queryResults;

                try {
                    System.out.printf("Partition Query: %s%n", query);
                    queryResults = _newRelicAdapter.runQuery(query);
                } catch (IOException e) {
                    LOGGER.error("Error when attempting to query NRQL for metrics.", e);
                    //throw new SamplingException("Could not query metrics from NRQL.");
                    continue;
                }

                for (NewRelicQueryResult result : queryResults) {
                    try {
                        resultCounts[0] += addPartitionMetrics(result);
                    } catch (InvalidNewRelicResultException e) {
                        // Unlike PrometheusMetricSampler, this form of exception is probably very unlikely since
                        // we will be getting cleaned up and well formed data directly from NRDB, but just keeping
                        // this check here anyway to be safe
                        LOGGER.trace("Invalid query result received from New Relic for partition query {}", query, e);
                        resultCounts[1]++;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error when converting topics to bins.", e);
        }
    }

    abstract static class KafkaSize implements Comparable<KafkaSize> {
        private int _size;

        KafkaSize(int size) {
            _size = size;
        }

        int getSize() {
            return _size;
        }

        @Override
        public int compareTo(KafkaSize other) {
            return _size - other.getSize();
        }

        @Override
        public String toString() {
            return String.format("KafkaSize with size: %s", _size);
        }
    }

    /**
     * Used to store the number of brokers in each topic.
     */
    private static class BrokerSize extends KafkaSize {
        private int _brokerId;

        BrokerSize(int size, int brokerId) {
            super(size);
            _brokerId = brokerId;
        }

         int getBrokerId() {
            return _brokerId;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            BrokerSize otherSize = (BrokerSize) other;
            return this.hashCode() == otherSize.hashCode();
        }

        @Override
        public int hashCode() {
            return Objects.hash(_brokerId, getSize());
        }

        @Override
        public String toString() {
            return String.format("BrokerSize with brokerId %s and size: %s", _brokerId, getSize());
        }
    }

    /**
     * Used to pair topics together with their size.
     * Note that size in this context refers to the number of
     * leaders and replicas of this topic.
     */
    private static class TopicSize extends KafkaSize {
        private String _topic;
        private int _brokerId;
        private boolean _isBrokerTopic;

        private TopicSize(String topic, int size) {
            super(size);
            _topic = topic;
            _isBrokerTopic = false;
        }

        private TopicSize(String topic, int size, int brokerId) {
            super(size);
            _topic = topic;
            _brokerId = brokerId;
            _isBrokerTopic = true;
        }

        private String getTopic() {
            return _topic;
        }

        private int getBrokerId() {
            return _brokerId;
        }

        private boolean getIsBrokerTopic() {
            return _isBrokerTopic;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            TopicSize topicSizeOther = (TopicSize) other;
            return hashCode() == topicSizeOther.hashCode();
        }

        @Override
        public int hashCode() {
            return Objects.hash(_topic, _brokerId, _isBrokerTopic, getSize());
        }

        @Override
        public String toString() {
            if (_isBrokerTopic) {
                return String.format("TopicSize with topic %s and size: %s",
                        _topic, getSize());
            } else {
                return String.format("TopicSize with brokerId %s, topic %s, and size: %s",
                        _brokerId, _topic, getSize());
            }
        }
    }

    abstract static class NewRelicQueryBin {
        private int _currentSize;
        private List<KafkaSize> _sizes;

        NewRelicQueryBin() {
            _sizes = new ArrayList<>();
            _currentSize = 0;
        }

        boolean addKafkaSize(KafkaSize newSize) {
            int size = newSize.getSize();
            if (_currentSize + size > MAX_SIZE) {
                return false;
            } else {
                _currentSize += size;
                _sizes.add(newSize);
                return true;
            }
        }

        List<KafkaSize> getSizes() {
            return _sizes;
        }

        abstract String generateStringForQuery();
    }

    static class BrokerQueryBin extends NewRelicQueryBin {
        BrokerQueryBin() {
            super();
        }

        @Override
        String generateStringForQuery() {
            if (getSizes().size() == 0) {
                return "";
            } else {
                // We want this to be comma separated list of brokers
                // Example: "WHERE broker IN (broker1, broker2, ...) "
                StringBuffer brokerBuffer = new StringBuffer();
                brokerBuffer.append("WHERE broker IN (");

                List<KafkaSize> sizes = getSizes();
                for (int i = 0; i < sizes.size() - 1; i++) {
                    BrokerSize brokerSize = (BrokerSize) sizes.get(i);
                    brokerBuffer.append(String.format("%s, ", brokerSize.getBrokerId()));
                }

                // Handle the last broker and add in parentheses + space instead of comma to finish
                BrokerSize brokerSize = (BrokerSize) sizes.get(sizes.size() - 1);
                brokerBuffer.append(String.format("%s) ", brokerSize.getBrokerId()));

                return brokerBuffer.toString();
            }
        }
    }

    static class TopicQueryBin extends NewRelicQueryBin {
        TopicQueryBin() {
            super();
        }

        /**
         * Given the list of all topics in this bin,
         * we generate a string of the topics separated by a comma and space
         * @return - String of topics separated by comma and space w/ no trailing comma or space
         */
        @Override
        String generateStringForQuery() {
            ArrayList<TopicSize> topics = new ArrayList<>();
            ArrayList<TopicSize> brokerTopics = new ArrayList<>();

            for (KafkaSize size: getSizes()) {
                TopicSize topicSize = (TopicSize) size;
                if (topicSize.getIsBrokerTopic()) {
                    brokerTopics.add(topicSize);
                } else {
                    topics.add(topicSize);
                }
            }
            // We want a comma on all but the last element so we will handle the last one separately
            // We want these topics to be in the format:
            // "topic IN ('topic1', 'topic2', ...)"
            StringBuffer topicBuffer = new StringBuffer();
            if (topics.size() > 0) {
                topicBuffer.append("topic IN (");

                for (int i = 0; i < topics.size() - 1; i++) {
                    topicBuffer.append(String.format("'%s', ", topics.get(i).getTopic()));
                }
                // Add in last element without a comma or space
                topicBuffer.append(String.format("'%s')", topics.get(topics.size() - 1).getTopic()));
            }

            // We want to combine broker topics into the format
            // "(topic = 'topic1' AND broker = brokerId1) OR (topic = 'topic2' AND broker = brokerId2) ..."
            StringBuffer topicBrokerBuffer = new StringBuffer();
            if (brokerTopics.size() > 0) {
                if (topics.size() > 0) {
                    topicBrokerBuffer.append(" OR ");
                }
                for (int i = 0; i < brokerTopics.size() - 1; i++) {
                    topicBrokerBuffer.append(String.format("(topic = '%s' AND broker = %s) OR ",
                            brokerTopics.get(i).getTopic(), brokerTopics.get(i).getBrokerId()));
                }
                // Add in last element without OR
                topicBrokerBuffer.append(String.format("(topic = '%s' AND broker = %s)",
                        brokerTopics.get(brokerTopics.size() - 1).getTopic(),
                        brokerTopics.get(brokerTopics.size() - 1).getBrokerId()));
            }

            return topicBuffer + topicBrokerBuffer.toString();
        }
    }

    private ArrayList<KafkaSize> getSortedBrokersByTopicCount(Cluster cluster) {
        ArrayList<KafkaSize> brokerSizes = new ArrayList<>();

        for (Node node: cluster.nodes()) {
            HashSet<String> topicsInNode = new HashSet<>();
            List<PartitionInfo> partitions = cluster.partitionsForNode(node.id());
            for (PartitionInfo partition: partitions) {
                topicsInNode.add(partition.topic());
            }
            brokerSizes.add(new BrokerSize(topicsInNode.size(), node.id()));
        }

        Collections.sort(brokerSizes);

        return brokerSizes;
    }

    private ArrayList<KafkaSize> getSortedTopicsByReplicaCount(Cluster cluster) {
        Set<String> topics = cluster.topics();

        // Get the total number of leaders + replicas that are for this topic
        // Note that each leader and replica is counted as separately
        // since they are on different brokers and will require a different output from NRQL
        ArrayList<KafkaSize> topicSizes = new ArrayList<>();
        for (String topic: topics) {
            int size = 0;
            for (PartitionInfo partitionInfo: cluster.partitionsForTopic(topic)) {
                size += partitionInfo.replicas().length;
            }

            // If topic has more than 2000 replicas, go through each broker and get
            // the count of replicas in that broker for this topic and create
            // a new topicSize for each broker, topic combination
            if (size > MAX_SIZE) {
                HashMap<Integer, Integer> brokerToCount = new HashMap<>();
                for (Node node: cluster.nodes()) {
                    brokerToCount.put(node.id(), 0);
                }
                for (PartitionInfo partitionInfo: cluster.partitionsForTopic(topic)) {
                    for (Node broker: partitionInfo.replicas()) {
                        brokerToCount.put(broker.id(), 1 + brokerToCount.get(broker.id()));
                    }
                }
                for (Map.Entry<Integer, Integer> entry: brokerToCount.entrySet()) {
                    if (entry.getValue() != 0) {
                        topicSizes.add(new TopicSize(topic, entry.getValue(), entry.getKey()));
                    }
                }
            } else {
                topicSizes.add(new TopicSize(topic, size));
            }
        }

        Collections.sort(topicSizes);

        return topicSizes;
    }

    private List<NewRelicQueryBin> assignToBins(List<KafkaSize> kafkaSizes, Class<?> binType)
            throws InstantiationException, IllegalAccessException {
        List<NewRelicQueryBin> queryBins = new ArrayList<>();

        // Since topicSizes is ordered in ascending order, we traverse it backwards
        for (int i = kafkaSizes.size() - 1; i >= 0; i--) {
            KafkaSize kafkaSize = kafkaSizes.get(i);
            boolean added = false;
            for (NewRelicQueryBin queryBin: queryBins) {
                if (queryBin.addKafkaSize(kafkaSize)) {
                    added = true;
                    break;
                }
            }

            // If we couldn't add the topic to any of the previous bins,
            // create a new bin and add the topic to that bin
            if (!added) {
                NewRelicQueryBin newBin = (NewRelicQueryBin) binType.newInstance();
                added = newBin.addKafkaSize(kafkaSize);
                if (!added) {
                    LOGGER.error("Size object has too many items: {}",
                            kafkaSize);
                } else {
                    queryBins.add(newBin);
                }
            }
        }

        return queryBins;
    }

    private List<String> getPartitionQueries(List<NewRelicQueryBin> queryBins) {
        List<String> queries = new ArrayList<>();
        for (NewRelicQueryBin queryBin: queryBins) {
            queries.add(NewRelicQuerySupplier.partitionQuery(queryBin.generateStringForQuery()));
        }
        return queries;
    }

    private List<String> getTopicQueries(List<NewRelicQueryBin> queryBins) {
        List<String> queries = new ArrayList<>();
        for (NewRelicQueryBin queryBin: queryBins) {
            queries.add(NewRelicQuerySupplier.topicQuery(queryBin.generateStringForQuery()));
        }
        return queries;
    }

    private int addBrokerMetrics(NewRelicQueryResult queryResult)
            throws InvalidNewRelicResultException {
        int brokerID = queryResult.getBrokerID();
        long timeMs = queryResult.getTimeMs();

        int metricsAdded = 0;
        for (Map.Entry<RawMetricType, Double> entry: queryResult.getResults().entrySet()) {
            addMetricForProcessing(new BrokerMetric(entry.getKey(), timeMs,
                    brokerID, entry.getValue()));
            metricsAdded++;
        }
        return metricsAdded;
    }

    private int addTopicMetrics(NewRelicQueryResult queryResult)
            throws InvalidNewRelicResultException {
        int brokerID = queryResult.getBrokerID();
        String topic = queryResult.getTopic();
        long timeMs = queryResult.getTimeMs();

        int metricsAdded = 0;
        for (Map.Entry<RawMetricType, Double> entry: queryResult.getResults().entrySet()) {
            addMetricForProcessing(new TopicMetric(entry.getKey(), timeMs,
                    brokerID, topic, entry.getValue()));
            metricsAdded++;
        }
        return metricsAdded;
    }

    private int addPartitionMetrics(NewRelicQueryResult queryResult)
            throws InvalidNewRelicResultException {
        int brokerID = queryResult.getBrokerID();
        String topic = queryResult.getTopic();
        int partition = queryResult.getPartition();
        long timeMs = queryResult.getTimeMs();

        int metricsAdded = 0;
        for (Map.Entry<RawMetricType, Double> entry: queryResult.getResults().entrySet()) {
            addMetricForProcessing(new PartitionMetric(entry.getKey(), timeMs,
                    brokerID, topic, partition, entry.getValue()));
            metricsAdded++;
        }
        return metricsAdded;
    }

    @Override
    public void close() throws Exception {
        _httpClient.close();
    }
}
