/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic;

import java.util.ArrayList;
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

    private static final ArrayList<String> ALL_TOPICS_METRICS = new ArrayList<>();
    private static final ArrayList<String> BROKER_METRICS = new ArrayList<>();
    private static final ArrayList<String> TOPIC_METRICS = new ArrayList<>();

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

    private static String generateFeaturesWithSpecialAggregation(ArrayList<String> metrics,
                                                                 String aggregator, String additional) {
        StringBuffer buffer = new StringBuffer();

        // We want a comma on all but the last element so we will handle the last one separately
        for (String metric: metrics.subList(0, metrics.size() - 1)) {
            buffer.append(String.format("%s(%s) %s, ", aggregator, metric, additional));
        }
        // Add in last element without a comma or space
        buffer.append(String.format("%s(%s) %s", aggregator, metrics.get(metrics.size() - 1), additional));

        return buffer.toString();
    }

    private static String generateMaxFeatures(ArrayList<String> metrics) {
        StringBuffer buffer = new StringBuffer();

        // We want a comma on all but the last element so we will handle the last one separately
        for (String metric: metrics.subList(0, metrics.size() - 1)) {
            buffer.append(String.format("max(%s), ", metric));
        }
        // Add in last element without a comma or space
        buffer.append(String.format("max(%s)", metrics.get(metrics.size() - 1)));

        return buffer.toString();
    }

    static {
        // broker metrics
        ALL_TOPICS_METRICS.add("bytesInPerSec");
        ALL_TOPICS_METRICS.add("bytesOutPerSec");
        ALL_TOPICS_METRICS.add("replicationBytesInPerSec");
        ALL_TOPICS_METRICS.add("replicationBytesOutPerSec");
        ALL_TOPICS_METRICS.add("fetchRequestsPerSec");
        ALL_TOPICS_METRICS.add("produceRequestsPerSec");

        BROKER_METRICS.add("cpuTotalUtilizationPercentage");
        BROKER_METRICS.add("produceRequestsPerSec");
        BROKER_METRICS.add("fetchConsumerRequestsPerSec");
        BROKER_METRICS.add("fetchFollowerRequestsPerSec");
        BROKER_METRICS.add("requestQueueSize");
        BROKER_METRICS.add("responseQueueSize");
        BROKER_METRICS.add("produceQueueTimeMaxMs");
        BROKER_METRICS.add("produceQueueTimeMeanMs");
        BROKER_METRICS.add("produceQueueTime50thPercentileMs");
        BROKER_METRICS.add("produceQueueTime999thPercentileMs");
        BROKER_METRICS.add("fetchConsumerQueueTimeMaxMs");
        BROKER_METRICS.add("fetchConsumerQueueTimeMeanMs");
        BROKER_METRICS.add("fetchConsumerQueueTime50thPercentileMs");
        BROKER_METRICS.add("fetchConsumerQueueTime999thPercentileMs");
        BROKER_METRICS.add("fetchFollowerQueueTimeMaxMs");
        BROKER_METRICS.add("fetchFollowerQueueTimeMeanMs");
        BROKER_METRICS.add("fetchFollowerQueueTime50thPercentileMs");
        BROKER_METRICS.add("fetchFollowerQueueTime999thPercentileMs");
        BROKER_METRICS.add("produceLocalTimeMaxMs");
        BROKER_METRICS.add("produceLocalTimeMeanMs");
        BROKER_METRICS.add("produceLocalTime50thPercentileMs");
        BROKER_METRICS.add("produceLocalTime999thPercentileMs");
        BROKER_METRICS.add("fetchConsumerLocalTimeMaxMs");
        BROKER_METRICS.add("fetchConsumerLocalTimeMeanMs");
        BROKER_METRICS.add("fetchConsumerLocalTime50thPercentileMs");
        BROKER_METRICS.add("fetchConsumerLocalTime999thPercentileMs");
        BROKER_METRICS.add("fetchFollowerLocalTimeMaxMs");
        BROKER_METRICS.add("fetchFollowerLocalTimeMeanMs");
        BROKER_METRICS.add("fetchFollowerLocalTime50thPercentileMs");
        BROKER_METRICS.add("fetchFollowerLocalTime999thPercentileMs");
        BROKER_METRICS.add("produceLatencyMaxMs");
        BROKER_METRICS.add("produceLatencyMeanMs");
        BROKER_METRICS.add("produceLatency50thPercentileMs");
        BROKER_METRICS.add("produceLatency999thPercentileMs");
        BROKER_METRICS.add("fetchConsumerLatencyMaxMs");
        BROKER_METRICS.add("fetchConsumerLatencyMeanMs");
        BROKER_METRICS.add("fetchConsumerLatency50thPercentileMs");
        BROKER_METRICS.add("fetchConsumerLatency999thPercentileMs");
        BROKER_METRICS.add("fetchFollowerLatencyMaxMs");
        BROKER_METRICS.add("fetchFollowerLatencyMeanMs");
        BROKER_METRICS.add("fetchFollowerLatency50thPercentileMs");
        BROKER_METRICS.add("fetchFollowerLatency999thPercentileMs");
        BROKER_METRICS.add("logFlushOneMinuteRateMs");
        BROKER_METRICS.add("logFlushMaxMs");
        BROKER_METRICS.add("logFlushMeanMs");
        BROKER_METRICS.add("logFlush50thPercentileMs");
        BROKER_METRICS.add("logFlush999thPercentileMs");
        BROKER_METRICS.add("requestHandlerAvgIdlePercent");

        // topic metrics
        TOPIC_METRICS.add("bytesInPerSec");
        TOPIC_METRICS.add("bytesOutPerSec");
        // FIXME This seems to only be a broker level stat for us
        TOPIC_METRICS.add("replicationBytesInPerSec");
        // FIXME This seems to only be a broker level stat for us
        TOPIC_METRICS.add("replicationBytesOutPerSec");
        TOPIC_METRICS.add("totalFetchRequestsPerSec");
        TOPIC_METRICS.add("totalProduceRequestsPerSec");
        TOPIC_METRICS.add("messagesInPerSec");

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
    }

    @Override public Map<RawMetricType, String> get() {
        return TYPE_TO_QUERY;
    }
}