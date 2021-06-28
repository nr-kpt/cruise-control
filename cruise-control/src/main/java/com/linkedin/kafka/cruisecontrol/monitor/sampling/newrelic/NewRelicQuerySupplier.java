/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.MetricScope.BROKER;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.MetricScope.TOPIC;

/**
 * Contains the NRQL queries which will output broker, topic, and partition level
 * stats which are used by cruise control.
 */
public class NewRelicQuerySupplier implements Supplier<Map<RawMetricType.MetricScope, String>> {
    private static final Map<RawMetricType.MetricScope, String> TYPE_TO_QUERY = new HashMap<>();

    // Currently we are hardcoding this in -> later need to make it specific to whatever cluster
    // this cruise control instance is running on
    private static final String CLUSTER_NAME = "test-odd-wire-kafka";

    private static final HashMap<String, RawMetricType> BROKER_METRICS = new HashMap<>();
    private static final HashMap<String, RawMetricType> TOPIC_METRICS = new HashMap<>();
    private static final HashMap<String, RawMetricType> PARTITION_METRICS = new HashMap<>();

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

    private static final String PARTITION_QUERY = "FROM Metric "
            + "SELECT max(kafka_log_Log_Value_Size) "
            + "WHERE entity.name = '%s' "
            + "WHERE %s "
            + "FACET broker, topic, partition "
            + "SINCE 1 minute ago "
            + "LIMIT MAX";

    public static String brokerQuery(String select) {
        return String.format(BROKER_QUERY, select, CLUSTER_NAME);
    }

    public static String topicQuery(String select) {
        return String.format(TOPIC_QUERY, select, CLUSTER_NAME);
    }

    public static String partitionQuery(String topics) {
        return String.format(PARTITION_QUERY, CLUSTER_NAME, topics);
    }

    private static String generateFeatures(Map<String, RawMetricType> metrics) {
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
        // broker level metrics
        BROKER_METRICS.put("bytesInPerSec", ALL_TOPIC_BYTES_IN);
        BROKER_METRICS.put("bytesOutPerSec", ALL_TOPIC_BYTES_OUT);
        BROKER_METRICS.put("replicationBytesInPerSec", ALL_TOPIC_REPLICATION_BYTES_IN);
        BROKER_METRICS.put("replicationBytesOutPerSec", ALL_TOPIC_REPLICATION_BYTES_OUT);
        BROKER_METRICS.put("totalFetchRequestsPerSec", ALL_TOPIC_FETCH_REQUEST_RATE);
        BROKER_METRICS.put("totalProduceRequestsPerSec", ALL_TOPIC_PRODUCE_REQUEST_RATE);
        BROKER_METRICS.put("messagesInPerSec", ALL_TOPIC_MESSAGES_IN_PER_SEC);
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

        // topic level metrics
        TOPIC_METRICS.put("bytesInPerSec", TOPIC_BYTES_IN);
        TOPIC_METRICS.put("bytesOutPerSec", TOPIC_BYTES_OUT);
        // We don't collect the following data on a topic level so I'm not including them
        //TOPIC_METRICS.put("replicationBytesInPerSec", TOPIC_REPLICATION_BYTES_IN);
        //TOPIC_METRICS.put("replicationBytesOutPerSec", TOPIC_REPLICATION_BYTES_OUT);
        TOPIC_METRICS.put("totalFetchRequestsPerSec", TOPIC_FETCH_REQUEST_RATE);
        TOPIC_METRICS.put("totalProduceRequestsPerSec", TOPIC_PRODUCE_REQUEST_RATE);
        TOPIC_METRICS.put("messagesInPerSec", TOPIC_MESSAGES_IN_PER_SEC);

        // partition level metrics
        PARTITION_METRICS.put("kafka_log_Log_Value_Size", PARTITION_SIZE);

        // Create the actual queries we want to run and save them
        TYPE_TO_QUERY.put(BROKER, brokerQuery(generateFeatures(BROKER_METRICS)));
        TYPE_TO_QUERY.put(TOPIC, topicQuery(generateFeatures(TOPIC_METRICS)));
    }

    public static Map<String, RawMetricType> getBrokerMap() {
        return BROKER_METRICS;
    }

    public static Map<String, RawMetricType> getTopicMap() {
        return TOPIC_METRICS;
    }

    public static Map<String, RawMetricType> getPartitionMap() {
        return PARTITION_METRICS;
    }

    @Override public Map<RawMetricType.MetricScope, String> get() {
        return TYPE_TO_QUERY;
    }
}