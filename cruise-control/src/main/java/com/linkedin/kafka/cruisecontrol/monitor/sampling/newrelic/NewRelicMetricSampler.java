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
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;

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

    private void configureNewRelicAdapter(Map<String,?> configs) {
        final String apiKey = (String) configs.get(NEWRELIC_API_CONFIG);
        if (apiKey == null) {
            throw new ConfigException(String.format(
                    "%s config is required to have an API Key", NEWRELIC_API_CONFIG));
        }
        final int accountId = (int) configs.get(NEWRELIC_ACCOUNT_ID);

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
    private class TopicSize implements Comparable<TopicSize> {
        private String _topic;
        private int _size;

        private TopicSize(String topic, int size) {
            _topic = topic;
            _size = size;
        }

        private String getTopic() {
            return _topic;
        }

        private int getSize() {
            return _size;
        }

        @Override
        public int compareTo(TopicSize other) {
            return _size - other.getSize();
        }
    }

    private class PartitionQueryBin {
        private int MAX_SIZE = 2000;

        private int current_size;
        List<TopicSize> topics;

        private PartitionQueryBin() {
            current_size = 0;
            topics = new ArrayList<>();
        }

        private boolean addTopic(TopicSize topic) {
            int topic_size = topic.getSize();
            if (current_size + topic_size > MAX_SIZE) {
                return false;
            } else {
                current_size += topic_size;
                topics.add(topic);
                return true;
            }
        }

        private String generateTopicStringForQuery() {
            StringBuffer buffer = new StringBuffer();

            // We want a comma on all but the last element so we will handle the last one separately
            for (int i = 0; i < topics.size() - 1; i++) {
                buffer.append(String.format("'%s', ", topics.get(i).getTopic()));
            }
            // Add in last element without a comma or space
            buffer.append(String.format("'%s'", topics.get(topics.size() - 1).getTopic()));

            return buffer.toString();
        }
    }

    private ArrayList<TopicSize> getSortedTopicByReplicaCount(Cluster cluster) {
        // Get the number of partitions per topic per broker from cluster
        Set<String> topics = cluster.topics();

        // Get the total number of leaders + replicas that are for this topic
        // Note that each leader and replica is counted as separately
        // since they are on different brokers and will require a different output from NRQL
        ArrayList<TopicSize> topicSizes = new ArrayList<>();
        for (String topic: topics) {
            int size = 1;
            for (PartitionInfo partitionInfo: cluster.partitionsForTopic(topic)) {
                size += partitionInfo.replicas().length;
            }
            topicSizes.add(new TopicSize(topic, size));
        }

        // Sort PartitionCounts
        Collections.sort(topicSizes);

        return topicSizes;
    }

    /**
     * Using the first fit decreasing algorithm that is used to solve BinPacking
     * problems such as this one. See this link: https://sites.cs.ucsb.edu/~suri/cs130b/BinPacking
     * for more information on this algorithm and also the optimality of the algorithm.
     * @param topicSizes
     * @return
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
                if (!newBin.addTopic(topicSize)) {
                    // FIXME -> Later: call some function to handle topics
                    //  with more than 2000 partitionCount * replication_factor
                    LOGGER.info("Topic {} is too large. It has {} leaders + replicas.",
                            topicSize.getTopic(), topicSize.getSize());
                }
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
