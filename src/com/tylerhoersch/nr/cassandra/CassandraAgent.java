package com.tylerhoersch.nr.cassandra;

import com.newrelic.metrics.publish.Agent;
import com.newrelic.metrics.publish.util.Logger;
import com.tylerhoersch.nr.cassandra.templates.Cassandra2xInstances;
import com.tylerhoersch.nr.cassandra.templates.Cassandra2xMetrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CassandraAgent extends Agent {
    private static final Logger logger = Logger.getLogger(CassandraAgent.class);
    private static final String GUID = "com.tylerhoersch.nr.cassandra";
    private static final String VERSION = "1.0.0";
    private final String name;
    private final JMXRunner jmxRunner;

    public CassandraAgent(String name, JMXRunner jmxRunner) {
        super(GUID, VERSION);

        this.name = name;
        this.jmxRunner = jmxRunner;
    }

    @Override
    public String getAgentName() {
        return name;
    }

    @Override
    public void pollCycle() {
        List<Metric> metrics = new ArrayList<>();
        try {
            List<String> cassandraInstances = getCassandraInstances();

            int failedHostCount = 0;
            for(String instance : cassandraInstances) {
                try {
                    metrics.addAll(getCassandraMetrics(instance));
                } catch (IOException e) {
                    failedHostCount++;
                }
            }

            metrics.add(Cassandra2xMetrics.CreateDownHostsMetric(failedHostCount));

            metrics.stream()
                    .filter(m -> m.getValue() != null && !m.getValue().toString().equals("NaN"))
                    .forEach(m -> reportMetric(m.getName(), m.getValueType(), m.getValue()));
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
