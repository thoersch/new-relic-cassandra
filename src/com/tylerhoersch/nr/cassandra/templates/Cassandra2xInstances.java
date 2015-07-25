package com.tylerhoersch.nr.cassandra.templates;

import com.newrelic.metrics.publish.util.Logger;
import com.tylerhoersch.nr.cassandra.JMXRunner;
import com.tylerhoersch.nr.cassandra.JMXTemplate;

import javax.management.MBeanServerConnection;
import java.util.HashMap;
import java.util.Map;

public class Cassandra2xInstances implements JMXTemplate<Map<String, Boolean>> {

    private static final Logger logger = Logger.getLogger(Cassandra2xInstances.class);
    private static final String UP = "UP";

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Boolean> execute(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        Map<String, Boolean> instances = new HashMap<>();

        Map<String, String> states = jmxRunner.getAttribute(connection, "org.apache.cassandra.net", null, "FailureDetector", null, "SimpleStates");

        if (states == null) {
            logger.warn("No servers found from discovery host.");
            return instances;
        }

        states.entrySet().stream().map(s -> instances.put(s.getKey().substring(s.getKey().indexOf("/"+1)), s.getValue().equals(UP)));

        return instances;
    }
}
