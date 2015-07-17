package com.tylerhoersch.nr.cassandra;

import com.newrelic.metrics.publish.Agent;
import com.newrelic.metrics.publish.util.Logger;
import com.tylerhoersch.nr.cassandra.templates.Cassandra2xInstances;
import com.tylerhoersch.nr.cassandra.templates.Cassandra2xMetrics;

import java.util.ArrayList;
import java.util.List;

public class CassandraAgent extends Agent {
    private static final Logger logger = Logger.getLogger(CassandraAgent.class);
    private static final String GUID = "com.tylerhoersch.nr.cassandra";
    private static final String VERSION = "1.0.0";
    private static final String PORT = "7199";
    private final String name;
    private final String host;
    private final JMXRunner jmxRunner;

    public CassandraAgent(String name, String host) {
        super(GUID, VERSION);

        this.name = name;
        this.host = host;
        jmxRunner = new JMXRunner(host, PORT);
    }

    @Override
    public String getAgentName() {
        return name;
    }

    @Override
    public void pollCycle() {
        try {
            List<String> cassandraInstances = getCassandraInstances();
            for(String instance : cassandraInstances) {
                List<Metric> metrics = getCassandraMetrics(instance);
            }

        } catch (Exception e) {
            logger.error("Error Polling Cassandra: ", e);
        }
    }

    private List<String> getCassandraInstances() throws Exception {
        return jmxRunner.run(new Cassandra2xInstances());
    }

    private List<Metric> getCassandraMetrics(String instance) throws Exception {
        return jmxRunner.run(new Cassandra2xMetrics(instance));
    }
}
