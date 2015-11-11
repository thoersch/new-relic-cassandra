package com.tylerhoersch.nr.cassandra;

import com.newrelic.metrics.publish.Agent;
import com.newrelic.metrics.publish.util.Logger;
import com.tylerhoersch.nr.cassandra.templates.Cassandra2xFailures;
import com.tylerhoersch.nr.cassandra.templates.Cassandra2xInstances;
import com.tylerhoersch.nr.cassandra.templates.Cassandra2xMetrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CassandraAgent extends Agent {
    private static final Logger logger = Logger.getLogger(CassandraAgent.class);
    private static final String GUID = "com.tylerhoersch.nr.cassandra";
    private static final String VERSION = "1.0.0";
    private final JMXRunnerFactory jmxRunnerFactory;
    private final String name;
    private final List<String> hosts;
    private final String port;
    private final String username;
    private final String password;

    public CassandraAgent(JMXRunnerFactory jmxRunnerFactory,
                          String name,
                          List<String> hosts,
                          String port,
                          String username,
                          String password) {
        super(GUID, VERSION);

        this.jmxRunnerFactory = jmxRunnerFactory;
        this.name = name;
        this.hosts = hosts;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    @Override
    public String getAgentName() {
        return name;
    }

    @Override
    public void pollCycle() {
        List<Metric> metrics = new ArrayList<>();
        try {
            JMXRunner jmxRunner = jmxRunnerFactory.createJMXRunner(hosts, port, username, password);
            Map<String, Boolean> instances = getCassandraInstances(jmxRunner);
            metrics.addAll(getCassandraFailures(jmxRunner, instances));

            for(String instance : instances.keySet()) {
            	// Only collect Cassandra metrics if Cassandra node is up. Otherwise exception will be thrown
            	// and the rest of metrics will not be pushed to New Relic
            	if (instances.get(instance).booleanValue()) {
                	jmxRunner = jmxRunnerFactory.createJMXRunner(instance, port, username, password);
                    metrics.addAll(getCassandraMetrics(jmxRunner, instance));
            	}            	
            }

            metrics.stream().filter(m -> m.getValue() != null && !m.getValue().toString().equals("NaN"))
                    .forEach(m -> reportMetric(m.getName(), m.getValueType(), m.getValue()));
        } catch (Exception e) {
            logger.error("Error Polling Cassandra: ", e);
        }
    }

    private List<Metric> getCassandraFailures(JMXRunner jmxRunner, Map<String, Boolean> instancesStates) throws Exception {
        return jmxRunner.run(new Cassandra2xFailures(instancesStates));
    }

    private Map<String, Boolean> getCassandraInstances(JMXRunner jmxRunner) throws Exception {
        return jmxRunner.run(new Cassandra2xInstances());
    }

    private List<Metric> getCassandraMetrics(JMXRunner jmxRunner, String instance) throws Exception {
        return jmxRunner.run(new Cassandra2xMetrics(instance));
    }
}
