package com.tylerhoersch.nr.cassandra.templates;

import com.tylerhoersch.nr.cassandra.JMXRunner;
import com.tylerhoersch.nr.cassandra.JMXTemplate;
import com.tylerhoersch.nr.cassandra.Metric;

import javax.management.MBeanServerConnection;
import java.util.ArrayList;
import java.util.List;

public class Cassandra2xFailures implements JMXTemplate<List<Metric>> {

    private static final String DOWNTIME_GLOBAL = "Cassandra/global/Downtime";
    private static final String COUNT = "count";

    @Override
    public List<Metric> execute(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<Metric> metrics = new ArrayList<>();

        Integer downEndpointCount = jmxRunner.getAttribute(connection, "org.apache.cassandra.net", null, "FailureDetector", null, "DownEndpointCount");
        metrics.add(new Metric(DOWNTIME_GLOBAL, COUNT, downEndpointCount));

        return metrics;
    }
}
