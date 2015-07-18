package com.tylerhoersch.nr.cassandra;

import com.newrelic.metrics.publish.configuration.ConfigurationException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class CassandraAgentFactoryTest {

    private CassandraAgentFactory cassandraAgentFactory;
    private Map<String, Object> configuration;

    @Before
    public void setup() {
        cassandraAgentFactory = new CassandraAgentFactory();
        configuration = new HashMap<>();
        configuration.put("name", "junit");
        configuration.put("host", "1234");
        configuration.put("port", "1234");
        configuration.put("username", "bilbo");
        configuration.put("password", "keepitsecret");
    }

    @Test(expected = ConfigurationException.class)
    public void verifyMissingHostThrowsConfigurationException() throws ConfigurationException {
        configuration.remove("host");
        cassandraAgentFactory.createConfiguredAgent(configuration);

        fail();
    }

    @Test
    public void verifyMissingCredentialsReturnsValidAgent() throws ConfigurationException {
        configuration.remove("username");
        configuration.remove("password");
        CassandraAgent cassandraAgent = (CassandraAgent) cassandraAgentFactory.createConfiguredAgent(configuration);

        assertNotNull(cassandraAgent);
    }
}
