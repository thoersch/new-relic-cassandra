package com.tylerhoersch.nr.cassandra;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.QueryExp;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class JMXRunnerTest {
    private MBeanServerConnection mBeanServerConnection = mock(MBeanServerConnection.class);
    private JMXRunner jmxRunner;

    @Before
    public void setup() {
        jmxRunner = new JMXRunner("127.0.0.1", "7199", "username", "password");
    }

    @Test
    public void verifyQueryWithNoMBeansReturnsNull() throws Exception {
        when(mBeanServerConnection.queryMBeans(any(ObjectName.class), any(QueryExp.class))).thenReturn(new HashSet<ObjectInstance>());
        Object attribute = jmxRunner.getAttribute(mBeanServerConnection, "domain", "name", "type", "scope", "attribute");

        assertNull(attribute);
    }

    @Test
    public void verifyObjectNameCreation() throws Exception {
        Object attribute = jmxRunner.getAttribute(mBeanServerConnection, "domain", "name", "type", "scope", "attribute");
        when(mBeanServerConnection.queryMBeans(any(ObjectName.class), any(QueryExp.class))).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ObjectName objectName = (ObjectName)invocation.getArguments()[0];
                assertEquals("name", objectName.getKeyProperty("name"));
                assertEquals("type", objectName.getKeyProperty("type"));
                assertEquals("scope", objectName.getKeyProperty("scope"));
                return null;
            }
        });
    }

    @Test
    public void verifyQueryWithBeansCallsForAttributes() throws Exception {
        final ObjectName objectName = new ObjectName("domain", "key", "value");
        Set<ObjectInstance> instances = new HashSet<>();
        instances.add(new ObjectInstance(objectName, "Object"));
        when(mBeanServerConnection.queryMBeans(any(ObjectName.class), any(QueryExp.class))).thenReturn(instances);
        jmxRunner.getAttribute(mBeanServerConnection, objectName, "attribute");

        verify(mBeanServerConnection, times(1)).queryMBeans(objectName, null);
        verify(mBeanServerConnection, times(1)).getAttribute(objectName, "attribute");
    }
}
