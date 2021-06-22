/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model;

public class NewRelicResultValue {
    private final double _value;

    NewRelicResultValue(double value) {
        _value = value;
    }

    public double getValue() {
        return _value;
    }

    @Override
    public String toString() {
        return String.format(" Value: %s", _value);
    }
}
