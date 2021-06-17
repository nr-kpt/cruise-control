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

    private static final String ALL_TOPIC_SELECT_QUERY = "FROM KafkaBrokerStats "
            + "SELECT average(%s) * uniqueCount(broker) "
            + "WHERE cluster = '%s' "
            + "SINCE 1 minute ago "
            + "LIMIT MAX";

    private static final String BROKER_QUERY = "FROM KafkaBrokerStats "
            + "SELECT max(%s) "
            + "WHERE cluster = '%s' "
            + "FACET broker "
            + "SINCE 1 minute ago "
            + "LIMIT MAX";

    // Using this query for queue and local time + logFlush stats
    // Created this new Stats value in NRDB since KafkaBrokerStats became
    // extremely cluttered otherwise
    private static final String CC_BROKER_QUERY = "FROM KafkaBrokerCruiseControlStats "
            + "SELECT max(%s) "
            + "WHERE cluster = '%s' "
            + "FACET broker "
            + "SINCE 1 minute ago "
            + "LIMIT MAX";

    private static final String TOPIC_QUERY = "FROM KafkaBrokerTopicStats "
            + "SELECT max(%s) "
            + "WHERE cluster = '%s' "
            + "AND topic is NOT NULL "
            + "FACET broker, topic "
            + "SINCE 1 minute ago "
            + "LIMIT MAX";

    private static String allTopicQuery(String selectFeature) {
        return String.format(ALL_TOPIC_SELECT_QUERY, selectFeature, CLUSTER_NAME);
    }

    private static String ccBrokerQuery(String selectFeature) {
        return String.format(CC_BROKER_QUERY, selectFeature, CLUSTER_NAME);
    }

    private static String brokerQuery(String selectFeature) {
        return String.format(BROKER_QUERY, selectFeature, CLUSTER_NAME);
    }

    private static String topicQuery(String selectFeature) {
        return String.format(TOPIC_QUERY, selectFeature, CLUSTER_NAME);
    }

    static {
        // broker metrics
        TYPE_TO_QUERY.put(BROKER_CPU_UTIL,
                String.format("FROM KafkaBrokerStats "
                        + "SELECT max(cpuTotalUtilizationPercentage) "
                        + "WHERE cluster = '%s' "
                        + "FACET broker "
                        + "SINCE 1 minute ago "
                        + "LIMIT MAX", CLUSTER_NAME));
        TYPE_TO_QUERY.put(ALL_TOPIC_BYTES_IN,
                allTopicQuery("bytesInPerSec"));
        TYPE_TO_QUERY.put(ALL_TOPIC_BYTES_OUT,
                allTopicQuery("bytesOutPerSec"));
        TYPE_TO_QUERY.put(ALL_TOPIC_REPLICATION_BYTES_IN,
                allTopicQuery("replicationBytesInPerSec"));
        TYPE_TO_QUERY.put(ALL_TOPIC_REPLICATION_BYTES_OUT,
                allTopicQuery("replicationBytesOutPerSec"));
        TYPE_TO_QUERY.put(ALL_TOPIC_FETCH_REQUEST_RATE,
                allTopicQuery("fetchRequestsPerSec"));
        TYPE_TO_QUERY.put(ALL_TOPIC_PRODUCE_REQUEST_RATE,
                allTopicQuery("produceRequestsPerSec"));

        // messagesInPerSec is only in KafkaBrokerTopicStats rather than KafkaBrokerStats
        // so we needed to create a custom query for this one
        TYPE_TO_QUERY.put(ALL_TOPIC_MESSAGES_IN_PER_SEC,
                String.format("FROM KafkaBrokerTopicStats "
                        + "SELECT average(messagesInPerSec) * uniqueCount(broker) "
                        + "WHERE cluster = '%s' "
                        + "AND topic IS NULL "
                        + "SINCE 1 minute ago", CLUSTER_NAME));

        TYPE_TO_QUERY.put(BROKER_PRODUCE_REQUEST_RATE,
                brokerQuery("produceRequestsPerSec"));
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_REQUEST_RATE,
                brokerQuery("fetchConsumerRequestsPerSec"));
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_REQUEST_RATE,
                brokerQuery("fetchFollowerRequestsPerSec"));
        TYPE_TO_QUERY.put(BROKER_REQUEST_QUEUE_SIZE,
                brokerQuery("requestQueueSize"));
        TYPE_TO_QUERY.put(BROKER_RESPONSE_QUEUE_SIZE,
                brokerQuery("responseQueueSize"));
        TYPE_TO_QUERY.put(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX,
                ccBrokerQuery("produceQueueTimeMaxMs"));
        TYPE_TO_QUERY.put(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN,
                ccBrokerQuery("produceQueueTimeMeanMs"));
        TYPE_TO_QUERY.put(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH,
                ccBrokerQuery("produceQueueTime50thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH,
                ccBrokerQuery("produceQueueTime999thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX,
                ccBrokerQuery("fetchConsumerQueueTimeMaxMs"));
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN,
                ccBrokerQuery("fetchConsumerQueueTimeMeanMs"));
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH,
                ccBrokerQuery("fetchConsumerQueueTime50thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH,
                ccBrokerQuery("fetchConsumerQueueTime999thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX,
                ccBrokerQuery("fetchFollowerQueueTimeMaxMs"));
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN,
                ccBrokerQuery("fetchFollowerQueueTimeMeanMs"));
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH,
                ccBrokerQuery("fetchFollowerQueueTime50thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH,
                ccBrokerQuery("fetchFollowerQueueTime999thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_PRODUCE_LOCAL_TIME_MS_MAX,
                ccBrokerQuery("produceLocalTimeMaxMs"));
        TYPE_TO_QUERY.put(BROKER_PRODUCE_LOCAL_TIME_MS_MEAN,
                ccBrokerQuery("produceLocalTimeMeanMs"));
        TYPE_TO_QUERY.put(BROKER_PRODUCE_LOCAL_TIME_MS_50TH,
                ccBrokerQuery("produceLocalTime50thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_PRODUCE_LOCAL_TIME_MS_999TH,
                ccBrokerQuery("produceLocalTime999thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX,
                ccBrokerQuery("fetchConsumerLocalTimeMaxMs"));
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN,
                ccBrokerQuery("fetchConsumerLocalTimeMeanMs"));
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH,
                ccBrokerQuery("fetchConsumerLocalTime50thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH,
                ccBrokerQuery("fetchConsumerLocalTime999thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX,
                ccBrokerQuery("fetchFollowerLocalTimeMaxMs"));
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN,
                ccBrokerQuery("fetchFollowerLocalTimeMeanMs"));
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH,
                ccBrokerQuery("fetchFollowerLocalTime50thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH,
                ccBrokerQuery("fetchFollowerLocalTime999thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_PRODUCE_TOTAL_TIME_MS_MAX,
                brokerQuery("produceLatencyMaxMs"));
        TYPE_TO_QUERY.put(BROKER_PRODUCE_TOTAL_TIME_MS_MEAN,
                brokerQuery("produceLatencyMeanMs"));
        TYPE_TO_QUERY.put(BROKER_PRODUCE_TOTAL_TIME_MS_50TH,
                brokerQuery("produceLatency50thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_PRODUCE_TOTAL_TIME_MS_999TH,
                brokerQuery("produceLatency999thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX,
                brokerQuery("fetchConsumerLatencyMaxMs"));
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN,
                brokerQuery("fetchConsumerLatencyMeanMs"));
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH,
                brokerQuery("fetchConsumerLatency50thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH,
                brokerQuery("fetchConsumerLatency999thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX,
                brokerQuery("fetchFollowerLatencyMaxMs"));
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN,
                brokerQuery("fetchFollowerLatencyMeanMs"));
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH,
                brokerQuery("fetchFollowerLatency50thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH,
                brokerQuery("fetchFollowerLatency999thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_LOG_FLUSH_RATE,
                ccBrokerQuery("logFlushOneMinuteRateMs"));
        TYPE_TO_QUERY.put(BROKER_LOG_FLUSH_TIME_MS_MAX,
                ccBrokerQuery("logFlushMaxMs"));
        TYPE_TO_QUERY.put(BROKER_LOG_FLUSH_TIME_MS_MEAN,
                ccBrokerQuery("logFlushMeanMs"));
        TYPE_TO_QUERY.put(BROKER_LOG_FLUSH_TIME_MS_50TH,
                ccBrokerQuery("logFlush50thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_LOG_FLUSH_TIME_MS_999TH,
                ccBrokerQuery("logFlush999thPercentileMs"));
        TYPE_TO_QUERY.put(BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT,
                brokerQuery("requestHandlerAvgIdlePercent"));

        // topic metrics
        TYPE_TO_QUERY.put(TOPIC_BYTES_IN,
                topicQuery("bytesInPerSec"));
        TYPE_TO_QUERY.put(TOPIC_BYTES_OUT,
                topicQuery("bytesOutPerSec"));
        TYPE_TO_QUERY.put(TOPIC_REPLICATION_BYTES_IN,
                // FIXME I don't think data is being collected for this metric
                topicQuery("replicationBytesInPerSec"));
        TYPE_TO_QUERY.put(TOPIC_REPLICATION_BYTES_OUT,
                // FIXME I don't think data is being collected for this metric
                topicQuery("replicationBytesOutPerSec"));
        TYPE_TO_QUERY.put(TOPIC_FETCH_REQUEST_RATE,
                topicQuery("totalFetchRequestsPerSec"));
        TYPE_TO_QUERY.put(TOPIC_PRODUCE_REQUEST_RATE,
                topicQuery("totalProduceRequestsPerSec"));
        TYPE_TO_QUERY.put(TOPIC_MESSAGES_IN_PER_SEC,
                topicQuery("messagesInPerSec"));

        // partition metrics
        TYPE_TO_QUERY.put(PARTITION_SIZE,
                // FIXME
                "kafka_log_Log_Value{name=\"Size\",topic!=\"\",partition!=\"\"}");
    }

    @Override public Map<RawMetricType, String> get() {
        return TYPE_TO_QUERY;
    }
}