package com.tylerhoersch.nr.cassandra.templates;

import com.newrelic.metrics.publish.util.Logger;
import com.tylerhoersch.nr.cassandra.AttributeJMXRequestMapper;
import com.tylerhoersch.nr.cassandra.AttributeType;
import com.tylerhoersch.nr.cassandra.JMXRunner;
import com.tylerhoersch.nr.cassandra.JMXTemplate;

import javax.management.MBeanServerConnection;
import java.util.HashMap;
import java.util.Map;

public class CassandraInstances implements JMXTemplate<Map<String, Boolean>> {

    private static final Logger logger = Logger.getLogger(CassandraInstances.class);
    private static final String UP = "UP";

    private final AttributeJMXRequestMapper attributeMap;

    public CassandraInstances(AttributeJMXRequestMapper attributeMap) {
        this.attributeMap = attributeMap;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Boolean> execute(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        Map<String, Boolean> instances = new HashMap<>();

        Map<String, String> states = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.HOST_STATES));

        if (states == null) {
            logger.warn("No servers found from discovery host.");
            return instances;
        }

        states.entrySet().stream().forEach(s -> instances.put(s.getKey().substring(s.getKey().indexOf("/")+1), s.getValue().equals(UP)));

        return instances;
    }
}
