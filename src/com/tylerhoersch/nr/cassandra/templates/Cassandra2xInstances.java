package com.tylerhoersch.nr.cassandra.templates;

import com.tylerhoersch.nr.cassandra.JMXRunner;
import com.tylerhoersch.nr.cassandra.JMXTemplate;

import javax.management.MBeanServerConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Cassandra2xInstances implements JMXTemplate<List<String>> {
    @SuppressWarnings("unchecked")
    @Override
    public List<String> execute(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<String> instances = new ArrayList<>();

        Map scores = jmxRunner.getAttribute(connection, "org.apache.cassandra.db", null, "DynamicEndpointSnitch", null, "Scores");

        if (scores == null) {
            return instances;
        }

        instances = (List<String>) scores.keySet().stream().map(s -> s.toString().substring(s.toString().indexOf("/")+1, s.toString().length())).collect(Collectors.toList());

        return instances;
    }
}
