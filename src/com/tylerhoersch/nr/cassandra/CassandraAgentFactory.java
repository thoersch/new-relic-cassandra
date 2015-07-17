package com.tylerhoersch.nr.cassandra;

import com.newrelic.metrics.publish.Agent;
import com.newrelic.metrics.publish.AgentFactory;
import com.newrelic.metrics.publish.configuration.ConfigurationException;

import java.util.Map;

public class CassandraAgentFactory extends AgentFactory {
    @Override
    public Agent createConfiguredAgent(Map<String, Object> map) throws ConfigurationException {
        String name = (String) map.get("name");
        String host = (String) map.get("host");
        String port = (String) map.get("port");
        String username = map.get("username") == null ? null : (String)map.get("username");
        String password = map.get("password") == null ? null : (String)map.get("password");

        if (name == null || host == null) {
            throw new ConfigurationException("'name' and 'host' cannot be null.");
        }

        return new CassandraAgent(name, host, port, username, password);
    }
}
