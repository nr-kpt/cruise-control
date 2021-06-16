/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model;

import com.fasterxml.jackson.databind.JsonNode;
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

    private static final Set<String> RESERVED_ATTRS = new HashSet<>();
    static {
        RESERVED_ATTRS.add(BEGIN_TIME_SECONDS_ATTR);
        RESERVED_ATTRS.add(END_TIME_SECONDS_ATTR);
        RESERVED_ATTRS.add(FACET_ATTR);
    }

    private final long _beginTimeSeconds;
    private final long _endTimeSeconds;

    private final List<String> _facets = new ArrayList<>();

    private final Map<String, NewRelicResultValue> _results = new HashMap<>();

    public NewRelicQueryResult(JsonNode result) {
        _beginTimeSeconds = result.get(BEGIN_TIME_SECONDS_ATTR).asLong();
        _endTimeSeconds = result.get(END_TIME_SECONDS_ATTR).asLong();

        for (JsonNode facet : result.get(FACET_ATTR)) {
            _facets.add(facet.asText());
        }

        Iterator<String> fieldNames = result.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();

            if (RESERVED_ATTRS.contains(fieldName)) {
                continue;
            }

            _results.put(fieldName, new NewRelicResultValue(fieldName, result.get(fieldName).asDouble()));
        }
    }

    @Override
    public String toString() {
        return "Results: " + _results.toString();
    }

    public long getBeginTimeSeconds() {
        return _beginTimeSeconds;
    }

    public long getEndTimeSeconds() {
        return _endTimeSeconds;
    }

    public List<String> getFacets() {
        return _facets;
    }

    public Map<String, NewRelicResultValue> getResults() {
        return _results;
    }
}
