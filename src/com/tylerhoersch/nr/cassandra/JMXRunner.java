package com.tylerhoersch.nr.cassandra;

import com.newrelic.metrics.publish.util.Logger;

import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

public class JMXRunner {
    private final static Logger logger = Logger.getLogger(JMXRunner.class);
    private final static String JMX_URL_FORMAT = "service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi";
    private final static String OBJECT_NAME_NAME = "name";
    private final static String OBJECT_NAME_TYPE = "type";
    private final static String OBJECT_NAME_SCOPE = "scope";

    private final String host;
    private final String port;
    private final String username;
    private final String password;

    public JMXRunner(String host, String port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public <T> T run(JMXTemplate<T> template) throws Exception {
        JMXConnector connector = null;
        T value = null;

        try {
            JMXServiceURL address = new JMXServiceURL(String.format(JMX_URL_FORMAT, host, port));
            try {
                Map<String, String[]> environment = getEnvironmentMap();
                connector = environment == null
                        ? JMXConnectorFactory.connect(address)
                        : JMXConnectorFactory.connect(address, environment);
            } catch (IOException ex) {
                throw new Exception(host, ex);
            }
            MBeanServerConnection mbs = connector.getMBeanServerConnection();
            value = template.execute(mbs, this);
        } catch (Exception e) {
            logger.error(e);
            throw e;
        } finally {
            close(connector);
        }

        return value;
    }

    public <T> T getAttribute(MBeanServerConnection connection, String domain, String name, String type, String scope, String attribute) throws Exception {
        return getAttribute(connection, createObjectName(domain, name, type, scope), attribute);
    }

    @SuppressWarnings("unchecked")
    public <T> T getAttribute(MBeanServerConnection connection, ObjectName objectName, String attribute) throws Exception {
        Set<ObjectInstance> instances = queryConnectionBy(connection, objectName);
        if(instances == null || instances.size() == 0) {
            return null;
        }

        return (T) connection.getAttribute(objectName, attribute);
    }

    private Set<ObjectInstance> queryConnectionBy(MBeanServerConnection connection, ObjectName objectName) throws Exception {
        return connection.queryMBeans(objectName, null);
    }

    private Map<String, String[]> getEnvironmentMap() {
        if(username == null || password == null) {
            return null;
        }

        Map<String, String[]> environment = new Hashtable<>();
        String[] credentials = {username, password};
        environment.put(JMXConnector.CREDENTIALS, credentials);
        return environment;
    }

    private void close(JMXConnector connector) {
        if (connector == null) {
            return;
        }

        try {
            connector.close();
        } catch (IOException e) {
            logger.error("Error closing JMX connection: ", e);
        }
    }

    private ObjectName createObjectName(String domain, String name, String type, String scope) throws Exception {
        Hashtable<String,String> table = new Hashtable<>();
        if (name != null) table.put(OBJECT_NAME_NAME, name);
        if (type != null) table.put(OBJECT_NAME_TYPE, type);
        if (scope != null) table.put(OBJECT_NAME_SCOPE, scope);
        return ObjectName.getInstance(domain, table);
    }

}
