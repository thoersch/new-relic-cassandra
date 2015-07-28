package com.tylerhoersch.nr.cassandra.templates;

import com.tylerhoersch.nr.cassandra.JMXRunner;
import com.tylerhoersch.nr.cassandra.JMXTemplate;
import com.tylerhoersch.nr.cassandra.Metric;

import javax.management.MBeanServerConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Cassandra2xFailures implements JMXTemplate<List<Metric>> {

    private static final String DOWNTIME_INSTANCE = "Cassandra/hosts/%s/Downtime";
    private static final String DOWNTIME_GLOBAL = "Cassandra/global/Downtime";
    private static final String COUNT = "count";
    private final Map<String, Boolean> instancesStates;

    public Cassandra2xFailures(Map<String, Boolean> instancesStates) {
        this.instancesStates = instancesStates;
    }

    @Override
    public List<Metric> execute(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<Metric> metrics = new ArrayList<>();

        Integer downEndpointCount = jmxRunner.getAttribute(connection, "org.apache.cassandra.net", null, "FailureDetector", null, "DownEndpointCount");
        metrics.add(new Metric(DOWNTIME_GLOBAL, COUNT, downEndpointCount));

        for(Map.Entry<String, Boolean> instance : instancesStates.entrySet()) {
            Metric metric = new Metric(String.format(DOWNTIME_INSTANCE, instance.getKey()), COUNT, instance.getValue() ? 0 : 1);
            metrics.add(metric);
        }

        return metrics;
    }
}
