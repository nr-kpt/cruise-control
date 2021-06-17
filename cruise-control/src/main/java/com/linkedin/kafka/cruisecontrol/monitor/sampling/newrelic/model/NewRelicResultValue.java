/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic.model;

public class NewRelicResultValue {
    //private final String _aggregator;
    private final String _label;
    private final double _value;

    NewRelicResultValue(String fieldName, double value) {
        // The aggregator becomes really weird when you aggregate by
        // more than one thing (ie average(one_value) * uniqueCount(another_value)
        // So I think its just a better idea to store all that in the _label

        //String[] labelParts = fieldName.split("\\.", 2);
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
        return String.format("Label: %s, Value: %s", _label, _value);
    }
}
