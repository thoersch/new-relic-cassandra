package com.tylerhoersch.nr.cassandra;

public class Metric {
    private final String name;
    private final String valueType;
    private final Number value;

    public Metric(String name, String valueType, Number value) {
        this.name = name;
        this.valueType = valueType;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValueType() {
        return valueType;
    }

    public Number getValue() {
        return value;
    }
}
