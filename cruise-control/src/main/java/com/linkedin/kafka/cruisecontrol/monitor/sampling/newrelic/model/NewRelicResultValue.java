/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model;

public class NewRelicResultValue {
    private final String _aggregator;
    private final String _label;
    private final double _value;

    NewRelicResultValue(String fieldName, double value) {
        String[] labelParts = fieldName.split("\\.", 2);
        _aggregator = labelParts[0];
        _label = labelParts[1];

        this._value = value;
    }

    public String getAggregator() {
        return _aggregator;
    }

    public String getLabel() {
        return _label;
    }

    public double getValue() {
        return _value;
    }

    @Override
    public String toString() {
        return String.format("Label: %s Value: %s", _label, _value);
    }
}
