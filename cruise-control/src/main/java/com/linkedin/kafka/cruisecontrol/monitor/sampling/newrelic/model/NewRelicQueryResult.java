/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.NewRelicQuerySupplier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NewRelicQueryResult {

    public static final String BEGIN_TIME_SECONDS_ATTR = "beginTimeSeconds";
    public static final String END_TIME_SECONDS_ATTR = "endTimeSeconds";
    public static final String FACET_ATTR = "facet";
    public static final String CLUSTER = "cluster";
    public static final String BROKER = "broker";
    public static final String TOPIC = "topic";

    private static final Set<String> RESERVED_ATTRS = new HashSet<>();
    static {
        RESERVED_ATTRS.add(BEGIN_TIME_SECONDS_ATTR);
        RESERVED_ATTRS.add(END_TIME_SECONDS_ATTR);
        RESERVED_ATTRS.add(FACET_ATTR);
        RESERVED_ATTRS.add(BROKER);
        RESERVED_ATTRS.add(TOPIC);

        // Note that we don't need to collect this since Cruise Control
        // only looks at data from one cluster
        RESERVED_ATTRS.add(CLUSTER);
    }

    private final List<String> _facets = new ArrayList<>();

    private final Map<RawMetricType, NewRelicResultValue> _results = new HashMap<>();

    public NewRelicQueryResult(JsonNode result) {
        // If we facet on multiple attributes, facets will be an array
        // and have multiple elements. If we facet on only one element,
        // facet will be a singular element so we handle this case here
        Map<String, RawMetricType> valueToMetricMap = NewRelicQuerySupplier.getAllTopicMap();
        if (result.has(FACET_ATTR)) {
            JsonNode facets = result.get(FACET_ATTR);
            if (facets.getNodeType() == JsonNodeType.ARRAY) {
                for (JsonNode facet : facets) {
                    _facets.add(facet.asText());
                    valueToMetricMap = NewRelicQuerySupplier.getTopicMap();
                }
            } else {
                _facets.add(facets.asText());
                valueToMetricMap = NewRelicQuerySupplier.getBrokerMap();
            }
        }

        Iterator<String> fieldNames = result.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();

            if (RESERVED_ATTRS.contains(fieldName)) {
                continue;
            }

            String metricLabel = fieldName.split("\\.")[1];

            _results.put(valueToMetricMap.get(metricLabel), new NewRelicResultValue(fieldName, result.get(fieldName).asDouble()));
        }
    }

    @Override
    public String toString() {
        return "NewRelic Query Result: " + _results.toString();
    }

    public List<String> getFacets() {
        return _facets;
    }

    public Map<RawMetricType, NewRelicResultValue> getResults() {
        return _results;
    }
}
