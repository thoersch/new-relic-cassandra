package com.tylerhoersch.nr.cassandra;

import com.tylerhoersch.nr.cassandra.templates.Cassandra2xInstances;
import com.tylerhoersch.nr.cassandra.templates.Cassandra2xMetrics;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class CassandraAgentTest {

    private JMXRunner jmxRunner = mock(JMXRunner.class);
    private CassandraAgent cassandraAgent;

    @Test
    public void verifyMetricReportingForAllHosts() throws Exception {
        List<String> instances = new ArrayList<>();
        instances.add("1.2.3.4");
        instances.add("2.2.3.2");
        List<Metric> metrics = new ArrayList<>();
        metrics.add(new Metric("m1", "v1", new BigDecimal(3.14)));
        metrics.add(new Metric("m2", "v2", 123L));
        metrics.add(new Metric("m3", "v3", 5));
        cassandraAgent = spy(new CassandraAgent("junit", jmxRunner));
        doNothing().when(cassandraAgent).reportMetric(any(String.class), any(String.class), any(Number.class));
        when(jmxRunner.run(any(JMXTemplate.class))).thenAnswer((mock) -> {
            // required since mockito was not giving unique results for
            // any(Cassandr2xIntances.class) vs. any(Cassandra2xMetrics.class)
            if(mock.getArguments()[0] instanceof Cassandra2xInstances) {
                return instances;
            } else {
                return metrics;
            }
        });
        cassandraAgent.pollCycle();
        verify(cassandraAgent, times(instances.size() * metrics.size())).reportMetric(any(String.class), any(String.class), any(Number.class));
    }
}
