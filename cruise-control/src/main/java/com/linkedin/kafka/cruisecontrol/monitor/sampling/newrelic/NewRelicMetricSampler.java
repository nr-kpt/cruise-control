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
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.Collections;
import java.util.Objects;
import java.util.HashMap;


public class NewRelicMetricSampler extends AbstractMetricSampler {
    private static final Logger LOGGER = LoggerFactory.getLogger(NewRelicMetricSampler.class);

    // Config name visible to tests
    static final String NEWRELIC_ENDPOINT = "https://staging-api.newrelic.com";
    static final String NEWRELIC_API_CONFIG = "newrelic.api.key";
    static final String NEWRELIC_ACCOUNT_ID = "newrelic.account.id";

    protected NewRelicAdapter _newRelicAdapter;
    protected Map<RawMetricType.MetricScope, String> _metricToNewRelicQueryMap;
    private CloseableHttpClient _httpClient;

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        configureNewRelicAdapter(configs);
        configureQueryMap();
    }

    private void configureQueryMap() {
        _metricToNewRelicQueryMap = (new NewRelicQuerySupplier()).get();
    }

    private void configureNewRelicAdapter(Map<String, ?> configs) {
        final String apiKey = (String) configs.get(NEWRELIC_API_CONFIG);
        if (apiKey == null) {
            throw new ConfigException(String.format(
                    "%s config is required to have an API Key", NEWRELIC_API_CONFIG));
        }
        final int accountId = (Integer) configs.get(NEWRELIC_ACCOUNT_ID);

        _httpClient = HttpClients.createDefault();
        _newRelicAdapter = new NewRelicAdapter(_httpClient, NEWRELIC_ENDPOINT, accountId, apiKey);

    }

    // This function will run all our queries using NewRelicAdapter
    @Override
    protected int retrieveMetricsForProcessing(MetricSamplerOptions metricSamplerOptions) throws SamplingException {
        int metricsAdded = 0;
        int resultsSkipped = 0;
        for (Map.Entry<RawMetricType.MetricScope, String> metricToQueryEntry : _metricToNewRelicQueryMap.entrySet()) {
            final RawMetricType.MetricScope scope = metricToQueryEntry.getKey();
            final String query = metricToQueryEntry.getValue();
            final List<NewRelicQueryResult> queryResults;

            try {
                queryResults = _newRelicAdapter.runQuery(query);
            } catch (IOException e) {
                LOGGER.error("Error when attempting to query NRQL for metrics.", e);
                throw new SamplingException("Could not query metrics from NRQL.");
            }

            for (NewRelicQueryResult result : queryResults) {
                try {
                    switch (scope) {
                        case BROKER:
                            metricsAdded += addBrokerMetrics(result);
                            break;
                        case TOPIC:
                            metricsAdded += addTopicMetrics(result);
                            break;

                        // We are handling partition level case separately since NRQL has 2000 item limit and
                        // some partition level queries may have more than 2000 items
                        case PARTITION:
                        default:
                            // Not supported.
                            break;
                    }
                } catch (InvalidNewRelicResultException e) {
                    // Unlike PrometheusMetricSampler, this form of exception is probably very unlikely since
                    // we will be getting cleaned up and well formed data directly from NRDB, but just keeping
                    // this check here anyway to be safe
                    LOGGER.trace("Invalid query result received from New Relic for query {}", query, e);
                    resultsSkipped++;
                }
            }
        }
        // Handling partition level case separately by going through each topic and adding partition size metrics
        // just for that topic

        // Get the sorted list of topics by their leader + follower count for each partition
        List<TopicSize> topicSizes = getSortedTopicByReplicaCount(metricSamplerOptions.cluster());

        // Use FFD algorithm (more info at method header) to assign topicSizes to queries
        List<PartitionQueryBin> queryBins = assignToBins(topicSizes);

        // Generate the queries based on the bins that PartitionCounts were assigned to
        List<String> partitionQueries = getPartitionQueries(queryBins);

        // Run the partition queries
        for (String query: partitionQueries) {
            final List<NewRelicQueryResult> queryResults;

            try {
                queryResults = _newRelicAdapter.runQuery(query);
            } catch (IOException e) {
                LOGGER.error("Error when attempting to query NRQL for metrics.", e);
                throw new SamplingException("Could not query metrics from NRQL.");
            }

            for (NewRelicQueryResult result : queryResults) {
                try {
                    metricsAdded += addPartitionMetrics(result);
                } catch (InvalidNewRelicResultException e) {
                    // Unlike PrometheusMetricSampler, this form of exception is probably very unlikely since
                    // we will be getting cleaned up and well formed data directly from NRDB, but just keeping
                    // this check here anyway to be safe
                    LOGGER.trace("Invalid query result received from New Relic for partition query {}", query, e);
                    resultsSkipped++;
                }
            }
        }

        LOGGER.info("Added {} metric values. Skipped {} invalid query results.", metricsAdded, resultsSkipped);
        return metricsAdded;
    }

    /**
     * Used to pair topics together with their size.
     * Note that size in this context refers to the number of
     * leaders and replicas of this topic.
     */
    private static class TopicSize implements Comparable<TopicSize> {
        private String _topic;
        private int _size;
        private int _brokerId;
        private boolean _isBrokerTopic;

        private TopicSize(String topic, int size) {
            _topic = topic;
            _size = size;
            _isBrokerTopic = false;
        }

        private TopicSize(String topic, int size, int brokerId) {
            _topic = topic;
            _size = size;
            _brokerId = brokerId;
            _isBrokerTopic = true;
        }

        private String getTopic() {
            return _topic;
        }

        private int getSize() {
            return _size;
        }

        private int getBrokerId() {
            return _brokerId;
        }

        private boolean getIsBrokerTopic() {
            return _isBrokerTopic;
        }

        @Override
        public int compareTo(TopicSize other) {
            return _size - other.getSize();
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
            return compareTo(topicSizeOther) == 0
                    && _topic.equals(topicSizeOther._topic)
                    && _brokerId == topicSizeOther.getBrokerId();
        }

        @Override
        public int hashCode() {
            return Objects.hash(_topic, _size);
        }
    }

    private static class PartitionQueryBin {
        // NRQL Query output limit
        private static final int MAX_SIZE = 2000;

        private int _currentSize;
        List<TopicSize> _topics;
        List<TopicSize> _brokerTopics;

        private PartitionQueryBin() {
            _currentSize = 0;
            _topics = new ArrayList<>();
            _brokerTopics = new ArrayList<>();
        }

        /**
         * Attempts to add a new topic to this partition bin.
         * If the topic is too large, we won't add it to
         * this bin.
         * @param topic - Topic which we are attempting to add
         * @return - Whether or not we were able to add the topic
         * to this bin.
         */
        private boolean addTopic(TopicSize topic) {
            int topicSize = topic.getSize();
            if (_currentSize + topicSize > MAX_SIZE) {
                return false;
            } else {
                _currentSize += topicSize;
                if (topic.getIsBrokerTopic()) {
                    _brokerTopics.add(topic);
                } else {
                    _topics.add(topic);
                }
                return true;
            }
        }

        /**
         * Given the list of all topics in this bin,
         * we generate a string of the topics separated by a comma and space
         * @return - String of topics separated by comma and space w/ no trailing comma or space
         */
        private String generateTopicStringForQuery() {
            // We want a comma on all but the last element so we will handle the last one separately
            // We want these topics to be in the format:
            // "('topic1', 'topic2', ...)"
            StringBuffer topicBuffer = new StringBuffer();
            for (int i = 0; i < _topics.size() - 1; i++) {
                topicBuffer.append(String.format("'%s', ", _topics.get(i).getTopic()));
            }
            // Add in last element without a comma or space
            topicBuffer.append(String.format("'%s'", _topics.get(_topics.size() - 1).getTopic()));

            // We want to combine broker topics into the format
            // "OR (topic = 'topic1' AND broker = brokerId1) OR (topic = 'topic2' AND broker = brokerId2) ..."
            StringBuffer topicBrokerBuffer = new StringBuffer();
            for (int i = 0; i < _brokerTopics.size(); i++) {
                topicBrokerBuffer.append(String.format("OR (topic = '%s' AND broker = %s) ",
                        _brokerTopics.get(i).getTopic(), _brokerTopics.get(i).getBrokerId()));
            }

            return topicBuffer + topicBrokerBuffer.toString();
        }
    }

    private ArrayList<TopicSize> getSortedTopicByReplicaCount(Cluster cluster) {
        Set<String> topics = cluster.topics();

        // Get the total number of leaders + replicas that are for this topic
        // Note that each leader and replica is counted as separately
        // since they are on different brokers and will require a different output from NRQL
        ArrayList<TopicSize> topicSizes = new ArrayList<>();
        for (String topic: topics) {
            int size = 0;
            for (PartitionInfo partitionInfo: cluster.partitionsForTopic(topic)) {
                size += partitionInfo.replicas().length;
            }

            // If topic has more than 2000 replicas, go through each broker and get
            // the count of replicas in that broker for this topic and create
            // a new topicSize for each broker, topic combination
            if (size > 2000) {
                HashMap<Integer, Integer> brokerToCount = new HashMap<>();
                for (PartitionInfo partitionInfo: cluster.partitionsForTopic(topic)) {
                    for (Node broker: partitionInfo.replicas()) {
                        int brokerTotal = 1;
                        if (brokerToCount.containsKey(broker.id())) {
                            brokerTotal += brokerToCount.get(broker.id());
                        }
                        brokerToCount.put(broker.id(), brokerTotal);
                    }
                }
                for (Map.Entry<Integer, Integer> entry: brokerToCount.entrySet()) {
                    topicSizes.add(new TopicSize(topic, entry.getValue(), entry.getKey()));
                }
            } else {
                topicSizes.add(new TopicSize(topic, size));
            }
        }

        Collections.sort(topicSizes);

        return topicSizes;
    }

    /**
     * Using the first fit decreasing algorithm that is used to solve BinPacking
     * problems such as this one. See this link: https://sites.cs.ucsb.edu/~suri/cs130b/BinPacking
     * for more information on this algorithm and also the optimality of the algorithm.
     * @param topicSizes - List of topics paired with their number of leader and replica partitions
     * @return -> Assigning these topicSizes to different queries all with total leader and replica
     * counts below the limit requirement for NRQL.
     */
    private List<PartitionQueryBin> assignToBins(List<TopicSize> topicSizes) {
        List<PartitionQueryBin> queryBins = new ArrayList<>();

        // Since topicSizes is ordered in ascending order, we traverse it backwards
        for (int i = topicSizes.size() - 1; i >= 0; i--) {
            TopicSize topicSize = topicSizes.get(i);
            boolean added = false;
            for (PartitionQueryBin queryBin: queryBins) {
                if (queryBin.addTopic(topicSize)) {
                    added = true;
                    break;
                }
            }

            // If we couldn't add the topic to any of the previous bins,
            // create a new bin and add the topic to that bin
            if (!added) {
                PartitionQueryBin newBin = new PartitionQueryBin();
                queryBins.add(newBin);
            }
        }

        return queryBins;
    }

    private List<String> getPartitionQueries(List<PartitionQueryBin> queryBins) {
        List<String> queries = new ArrayList<>();
        for (PartitionQueryBin queryBin: queryBins) {
            queries.add(NewRelicQuerySupplier.partitionQuery(queryBin.generateTopicStringForQuery()));
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
