/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;

/**
 * Contains the NRQL queries which will output broker, topic, and partition level
 * stats which are used by cruise control.
 */
public class NewRelicQuerySupplier implements Supplier<Map<RawMetricType, String>> {
    private static final Map<RawMetricType, String> TYPE_TO_QUERY = new HashMap<>();

    // Currently we are hardcoding this in -> later need to make it specific to whatever cluster
    // this cruise control instance is running on
    private static final String CLUSTER_NAME = "test-odd-wire-kafka";

    private static final HashMap<String, RawMetricType> ALL_TOPICS_METRICS = new HashMap<>();
    private static final HashMap<String, RawMetricType> BROKER_METRICS = new HashMap<>();
    private static final HashMap<String, RawMetricType> TOPIC_METRICS = new HashMap<>();

    private static final String ALL_TOPIC_SELECT_QUERY = "FROM KafkaBrokerStats "
            + "SELECT %s "
            + "WHERE cluster = '%s' "
            + "SINCE 1 minute ago "
            + "LIMIT MAX";

    private static final String BROKER_QUERY = "FROM KafkaBrokerStats "
            + "SELECT %s "
            + "WHERE cluster = '%s' "
            + "FACET broker "
            + "SINCE 1 minute ago "
            + "LIMIT MAX";

    private static final String TOPIC_QUERY = "FROM KafkaBrokerTopicStats "
            + "SELECT %s "
            + "WHERE cluster = '%s' "
            + "AND topic is NOT NULL "
            + "FACET broker, topic "
            + "SINCE 1 minute ago "
            + "LIMIT MAX";

    private static String allTopicQuery(String selectFeature) {
        return String.format(ALL_TOPIC_SELECT_QUERY, selectFeature, CLUSTER_NAME);
    }

    private static String brokerQuery(String selectFeature) {
        return String.format(BROKER_QUERY, selectFeature, CLUSTER_NAME);
    }

    private static String topicQuery(String selectFeature) {
        return String.format(TOPIC_QUERY, selectFeature, CLUSTER_NAME);
    }

    private static String generateFeaturesWithSpecialAggregation(Map<String, RawMetricType> metrics,
                                                                 String aggregator, String additional) {
        StringBuffer buffer = new StringBuffer();

        // We want a comma on all but the last element so we will handle the last one separately
        String[] metricLabels = metrics.keySet().toArray(new String [0]);
        for (int i = 0; i < metricLabels.length - 1; i++) {
            buffer.append(String.format("%s(%s) %s, ", aggregator, metricLabels[i], additional));
        }
        // Add in last element without a comma or space
        buffer.append(String.format("%s(%s) %s", aggregator, metricLabels[metricLabels.length - 1], additional));

        return buffer.toString();
    }

    private static String generateMaxFeatures(Map<String, RawMetricType> metrics) {
        StringBuffer buffer = new StringBuffer();

        // We want a comma on all but the last element so we will handle the last one separately
        String[] metricLabels = metrics.keySet().toArray(new String [0]);
        for (int i = 0; i < metricLabels.length - 1; i++) {
            buffer.append(String.format("max(%s), ", metricLabels[i]));
        }
        // Add in last element without a comma or space
        buffer.append(String.format("max(%s)", metricLabels[metricLabels.length - 1]));

        return buffer.toString();
    }

    static {
        // broker metrics
        ALL_TOPICS_METRICS.put("bytesInPerSec", ALL_TOPIC_BYTES_IN);
        ALL_TOPICS_METRICS.put("bytesOutPerSec", ALL_TOPIC_BYTES_OUT);
        ALL_TOPICS_METRICS.put("replicationBytesInPerSec", ALL_TOPIC_REPLICATION_BYTES_IN);
        ALL_TOPICS_METRICS.put("replicationBytesOutPerSec", ALL_TOPIC_REPLICATION_BYTES_OUT);
        ALL_TOPICS_METRICS.put("fetchRequestsPerSec", ALL_TOPIC_FETCH_REQUEST_RATE);
        ALL_TOPICS_METRICS.put("produceRequestsPerSec", ALL_TOPIC_PRODUCE_REQUEST_RATE);

        BROKER_METRICS.put("cpuTotalUtilizationPercentage", BROKER_CPU_UTIL);
        BROKER_METRICS.put("produceRequestsPerSec", BROKER_PRODUCE_REQUEST_RATE);
        BROKER_METRICS.put("fetchConsumerRequestsPerSec", BROKER_CONSUMER_FETCH_REQUEST_RATE);
        BROKER_METRICS.put("fetchFollowerRequestsPerSec", BROKER_FOLLOWER_FETCH_REQUEST_RATE);
        BROKER_METRICS.put("requestQueueSize", BROKER_REQUEST_QUEUE_SIZE);
        BROKER_METRICS.put("responseQueueSize", BROKER_RESPONSE_QUEUE_SIZE);
        BROKER_METRICS.put("produceQueueTimeMaxMs", BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX);
        BROKER_METRICS.put("produceQueueTimeMeanMs", BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN);
        BROKER_METRICS.put("produceQueueTime50thPercentileMs", BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH);
        BROKER_METRICS.put("produceQueueTime999thPercentileMs", BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH);
        BROKER_METRICS.put("fetchConsumerQueueTimeMaxMs", BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX);
        BROKER_METRICS.put("fetchConsumerQueueTimeMeanMs", BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN);
        BROKER_METRICS.put("fetchConsumerQueueTime50thPercentileMs", BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH);
        BROKER_METRICS.put("fetchConsumerQueueTime999thPercentileMs", BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH);
        BROKER_METRICS.put("fetchFollowerQueueTimeMaxMs", BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX);
        BROKER_METRICS.put("fetchFollowerQueueTimeMeanMs", BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN);
        BROKER_METRICS.put("fetchFollowerQueueTime50thPercentileMs", BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH);
        BROKER_METRICS.put("fetchFollowerQueueTime999thPercentileMs", BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH);
        BROKER_METRICS.put("produceLocalTimeMaxMs", BROKER_PRODUCE_LOCAL_TIME_MS_MAX);
        BROKER_METRICS.put("produceLocalTimeMeanMs", BROKER_PRODUCE_LOCAL_TIME_MS_MEAN);
        BROKER_METRICS.put("produceLocalTime50thPercentileMs", BROKER_PRODUCE_LOCAL_TIME_MS_50TH);
        BROKER_METRICS.put("produceLocalTime999thPercentileMs", BROKER_PRODUCE_LOCAL_TIME_MS_999TH);
        BROKER_METRICS.put("fetchConsumerLocalTimeMaxMs", BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX);
        BROKER_METRICS.put("fetchConsumerLocalTimeMeanMs", BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN);
        BROKER_METRICS.put("fetchConsumerLocalTime50thPercentileMs", BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH);
        BROKER_METRICS.put("fetchConsumerLocalTime999thPercentileMs", BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH);
        BROKER_METRICS.put("fetchFollowerLocalTimeMaxMs", BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX);
        BROKER_METRICS.put("fetchFollowerLocalTimeMeanMs", BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN);
        BROKER_METRICS.put("fetchFollowerLocalTime50thPercentileMs", BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH);
        BROKER_METRICS.put("fetchFollowerLocalTime999thPercentileMs", BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH);
        BROKER_METRICS.put("produceLatencyMaxMs", BROKER_PRODUCE_TOTAL_TIME_MS_MAX);
        BROKER_METRICS.put("produceLatencyMeanMs", BROKER_PRODUCE_TOTAL_TIME_MS_MEAN);
        BROKER_METRICS.put("produceLatency50thPercentileMs", BROKER_PRODUCE_TOTAL_TIME_MS_50TH);
        BROKER_METRICS.put("produceLatency999thPercentileMs", BROKER_PRODUCE_TOTAL_TIME_MS_999TH);
        BROKER_METRICS.put("fetchConsumerLatencyMaxMs", BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX);
        BROKER_METRICS.put("fetchConsumerLatencyMeanMs", BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN);
        BROKER_METRICS.put("fetchConsumerLatency50thPercentileMs", BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH);
        BROKER_METRICS.put("fetchConsumerLatency999thPercentileMs", BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH);
        BROKER_METRICS.put("fetchFollowerLatencyMaxMs", BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX);
        BROKER_METRICS.put("fetchFollowerLatencyMeanMs", BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN);
        BROKER_METRICS.put("fetchFollowerLatency50thPercentileMs", BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH);
        BROKER_METRICS.put("fetchFollowerLatency999thPercentileMs", BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH);
        BROKER_METRICS.put("logFlushOneMinuteRateMs", BROKER_LOG_FLUSH_RATE);
        BROKER_METRICS.put("logFlushMaxMs", BROKER_LOG_FLUSH_TIME_MS_MAX);
        BROKER_METRICS.put("logFlushMeanMs", BROKER_LOG_FLUSH_TIME_MS_MEAN);
        BROKER_METRICS.put("logFlush50thPercentileMs", BROKER_LOG_FLUSH_TIME_MS_50TH);
        BROKER_METRICS.put("logFlush999thPercentileMs", BROKER_LOG_FLUSH_TIME_MS_999TH);
        BROKER_METRICS.put("requestHandlerAvgIdlePercent", BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT);

        // topic metrics
        TOPIC_METRICS.put("bytesInPerSec", TOPIC_BYTES_IN);
        TOPIC_METRICS.put("bytesOutPerSec", TOPIC_BYTES_OUT);
        // FIXME This seems to only be a broker level stat for us
        //TOPIC_METRICS.put("replicationBytesInPerSec", TOPIC_REPLICATION_BYTES_IN);
        // FIXME This seems to only be a broker level stat for us
        //TOPIC_METRICS.put("replicationBytesOutPerSec", TOPIC_REPLICATION_BYTES_OUT);
        TOPIC_METRICS.put("totalFetchRequestsPerSec", TOPIC_FETCH_REQUEST_RATE);
        TOPIC_METRICS.put("totalProduceRequestsPerSec", TOPIC_PRODUCE_REQUEST_RATE);
        TOPIC_METRICS.put("messagesInPerSec", TOPIC_MESSAGES_IN_PER_SEC);

        // FIXME Note that the RawMetricType of the following three queries is wrong
        TYPE_TO_QUERY.put(ALL_TOPIC_BYTES_IN,
                allTopicQuery(generateFeaturesWithSpecialAggregation(ALL_TOPICS_METRICS,
                        "average", "* uniqueCount(broker)")));
        TYPE_TO_QUERY.put(BROKER_CPU_UTIL,
                brokerQuery(generateMaxFeatures(BROKER_METRICS)));
        TYPE_TO_QUERY.put(TOPIC_BYTES_IN,
                topicQuery(generateMaxFeatures(TOPIC_METRICS)));

        // messagesInPerSec is only in KafkaBrokerTopicStats rather than KafkaBrokerStats
        // so we needed to create a custom query for this one
        TYPE_TO_QUERY.put(ALL_TOPIC_MESSAGES_IN_PER_SEC,
                String.format("FROM KafkaBrokerTopicStats "
                        + "SELECT average(messagesInPerSec) * uniqueCount(broker) "
                        + "WHERE cluster = '%s' "
                        + "AND topic IS NULL "
                        + "SINCE 1 minute ago", CLUSTER_NAME));

        // partition metrics - being handled separately
        // TYPE_TO_QUERY.put(PARTITION_SIZE,
        //        "kafka_log_Log_Value{name=\"Size\",topic!=\"\",partition!=\"\"}");

        // Create immutable sets of our hashmaps
    }

    public static Map<String, RawMetricType> getAllTopicMap() {
        return ALL_TOPICS_METRICS;
    }

    public static Map<String, RawMetricType> getBrokerMap() {
        return BROKER_METRICS;
    }

    public static Map<String, RawMetricType> getTopicMap() {
        return TOPIC_METRICS;
    }

    @Override public Map<RawMetricType, String> get() {
        return TYPE_TO_QUERY;
    }
}