/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic;

import com.linkedin.kafka.cruisecontrol.exception.SamplingException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.AbstractMetricSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSamplerOptions;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model.NewRelicQueryResult;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class NewRelicMetricSampler extends AbstractMetricSampler {
    private static final Logger LOGGER = LoggerFactory.getLogger(NewRelicMetricSampler.class);

    // Config name visible to tests
    static final String NEWRELIC_ENDPOINT_CONFIG = "";

    // Config name visible to tests
    static final String PROMETHEUS_QUERY_RESOLUTION_STEP_MS_CONFIG = "prometheus.query.resolution.step.ms";
    private static final Integer DEFAULT_PROMETHEUS_QUERY_RESOLUTION_STEP_MS = (int) TimeUnit.MINUTES.toMillis(1);

    // Config name visible to tests
    static final String NEWRELIC_QUERY_SUPPLIER_CONFIG = "prometheus.query.supplier";
    private static final Class<?> NEWRELIC_QUERY_SUPPLIER = NewRelicQuerySupplier.class;

    protected int _samplingIntervalMs;
    protected Map<String, Integer> _hostToBrokerIdMap = new HashMap<>();
    protected NewRelicAdapter _newRelicAdapter;
    protected Map<RawMetricType, String> _metricToNewRelicQueryMap;
    private CloseableHttpClient _httpClient;

    // First thing -> need ways to be configured (I don't think as much as prometheus metric sampler)
    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        //configureSamplingInterval(configs);
        //configurePrometheusAdapter(configs);
        //configureQueryMap(configs);
    }

    // This function will run all our queries using NewRelicAdapter
    // We will then take the queried results and add them to either Broker, Topic, or Partition metrics
    //          - Probably a good idea to do the above using three separate functions
    @Override
    protected int retrieveMetricsForProcessing(MetricSamplerOptions metricSamplerOptions) throws SamplingException {
        int metricsAdded = 0;
        int resultsSkipped = 0;
        for (Map.Entry<RawMetricType, String> metricToQueryEntry : _metricToNewRelicQueryMap.entrySet()) {
            final RawMetricType metricType = metricToQueryEntry.getKey();
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
                    switch (metricType.metricScope()) {
                        case BROKER:
                            metricsAdded += addBrokerMetrics(metricSamplerOptions.cluster(), metricType, result);
                            break;
                        case TOPIC:
                            metricsAdded += addTopicMetrics(metricSamplerOptions.cluster(), metricType, result);
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

        LOGGER.info("Added {} metric values. Skipped {} invalid query results.", metricsAdded, resultsSkipped);
        return metricsAdded;
    }

    private int addBrokerMetrics(Cluster cluster, RawMetricType metricType, NewRelicQueryResult queryResult)
            throws InvalidNewRelicResultException {
        return 0;
    }

    private int addTopicMetrics(Cluster cluster, RawMetricType metricType, NewRelicQueryResult queryResult)
            throws InvalidNewRelicResultException {
        return 0;
    }

    private int addPartitionMetrics(Cluster cluster, RawMetricType metricType, NewRelicQueryResult queryResult)
            throws InvalidNewRelicResultException {
        return 0;
    }

    @Override
    public void close() throws Exception {

    }
}
