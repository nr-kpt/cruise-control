/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model;

public class NewRelicResultValue {
    //private final String _aggregator;
    private final String _label;
    private final double _value;

    NewRelicResultValue(String fieldName, double value) {
        _label = fieldName;

        this._value = value;
    }

    //public String getAggregator() {
    //    return _aggregator;
    //}

    public String getLabel() {
        return _label;
    }

    public double getValue() {
        return _value;
    }

    @Override
    public String toString() {
        return String.format(" Value: %s", _value);
    }
}
