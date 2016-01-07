package com.tylerhoersch.nr.cassandra;

public interface AttributeJMXRequestMapper {
    boolean canHandleVersion(int version);
    JMXRequest get(AttributeType type);
}
