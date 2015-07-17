package com.tylerhoersch.nr.cassandra;

import javax.management.MBeanServerConnection;

public interface JMXTemplate<T> {
    T execute(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception;
}
