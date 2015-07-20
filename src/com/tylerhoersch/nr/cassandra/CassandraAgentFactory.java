package com.tylerhoersch.nr.cassandra;

import com.newrelic.metrics.publish.Agent;
import com.newrelic.metrics.publish.AgentFactory;
import com.newrelic.metrics.publish.configuration.ConfigurationException;

import java.util.Map;
import java.util.Objects;

public class CassandraAgentFactory extends AgentFactory {
    private static final String NAME = "name";
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    @Override
    public Agent createConfiguredAgent(Map<String, Object> map) throws ConfigurationException {
        String name = (String) map.get(NAME);
        String host = (String) map.get(HOST);
        String port = (String) map.get(PORT);
        String username = isNullOrEmpty(map.get(USERNAME)) ? null : (String)map.get(USERNAME);
        String password = isNullOrEmpty(map.get(PASSWORD)) ? null : (String)map.get(PASSWORD);

        if (name == null || host == null || port == null) {
            throw new ConfigurationException(String.format("'%s', '%s' and '%s' cannot be null.", USERNAME, PASSWORD, PORT));
        }

        return new CassandraAgent(name, new JMXRunner(host, port, username, password));
    }

    private boolean isNullOrEmpty(Object value) {
        return value == null || value.toString() == "";
    }
}