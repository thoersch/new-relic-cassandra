package com.tylerhoersch.nr.cassandra.templates;

import com.tylerhoersch.nr.cassandra.*;

import javax.management.MBeanServerConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CassandraFailures implements JMXTemplate<List<Metric>> {

    private static final String DOWNTIME_INSTANCE = "Cassandra/hosts/%s/Downtime";
    private static final String DOWNTIME_GLOBAL = "Cassandra/global/Downtime";
    private static final String COUNT = "count";
    private final Map<String, Boolean> instancesStates;
    private final AttributeJMXRequestMapper attributeMap;

    public CassandraFailures(Map<String, Boolean> instancesStates, AttributeJMXRequestMapper attributeMap) {
        this.instancesStates = instancesStates;
        this.attributeMap = attributeMap;
    }

    @Override
    public List<Metric> execute(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<Metric> metrics = new ArrayList<>();

        Integer downEndpointCount = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.HOST_DOWN_COUNT));
        metrics.add(new Metric(DOWNTIME_GLOBAL, COUNT, downEndpointCount));

        for(Map.Entry<String, Boolean> instance : instancesStates.entrySet()) {
            Metric metric = new Metric(String.format(DOWNTIME_INSTANCE, instance.getKey()), COUNT, instance.getValue() ? 0 : 1);
            metrics.add(metric);
        }

        return metrics;
    }
}
