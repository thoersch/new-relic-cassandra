package com.tylerhoersch.nr.cassandra.templates;

import com.tylerhoersch.nr.cassandra.JMXRunner;
import com.tylerhoersch.nr.cassandra.JMXTemplate;
import com.tylerhoersch.nr.cassandra.Metric;

import javax.management.MBeanServerConnection;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Cassandra2xMetrics implements JMXTemplate<List<Metric>> {

    private static final String READ_LATENCY_INSTANCE = "Cassandra/hosts/%s/Latency/Reads";
    private static final String WRITE_LATENCY_INSTANCE = "Cassandra/hosts/%s/Latency/Writes";
    private static final String READ_LATENCY_GLOBAL = "Cassandra/global/Latency/Reads";
    private static final String WRITE_LATENCY_GLOBAL = "Cassandra/global/Latency/Writes";

    private static final String COMPACTION_PENDING_TASKS = "Cassandra/hosts/%s/Compaction/PendingTasks";
    private static final String MEMTABLE_PENDING_TASKS = "Cassandra/hosts/%s/MemtableFlush/PendingTasks";

    private static final String STORAGE_LOAD_INSTANCE = "Cassandra/host/%s/Storage/Data";
    private static final String STORAGE_LOAD_GLOBAL = "Cassandra/global/Storage/Data";
    private static final String COMMIT_LOG_INSTANCE = "Cassandra/host/%s/Storage/CommitLog";
    private static final String COMMIT_LOG_GLOBAL = "Cassandra/global/Storage/CommitLog";

    private static final String MILLIS = "millis";

    private final String instance;

    public Cassandra2xMetrics(String instance) {
        this.instance = instance;
    }

    @Override
    public List<Metric> execute(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<Metric> metrics = new ArrayList<>();

        metrics.addAll(getLatencyMetrics(connection, jmxRunner));
        metrics.addAll(getSystemMetrics(connection, jmxRunner));
        metrics.addAll(getStorageMetrics(connection, jmxRunner));

        return metrics;
    }

    private List<Metric> getStorageMetrics(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<Metric> metrics = new ArrayList<>();

        BigDecimal load = jmxRunner.getAttribute(connection, "org.apache.cassandra.db", null, "StorageService", null, "Load");
        metrics.add(new Metric(String.format(STORAGE_LOAD_INSTANCE, instance), "bytes", load));
        metrics.add(new Metric(STORAGE_LOAD_GLOBAL, "bytes", load));

        long commitLogSize = jmxRunner.getAttribute(connection, "org.apache.cassandra.db", null, "Commitlog", null, "TotalCommitlogSize");
        metrics.add(new Metric(String.format(COMMIT_LOG_INSTANCE, instance), "bytes", commitLogSize));
        metrics.add(new Metric(COMMIT_LOG_GLOBAL, "bytes", commitLogSize));

        return metrics;
    }

    private List<Metric> getSystemMetrics(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<Metric> metrics = new ArrayList<>();

        int compactionPendingTasks = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "PendingTasks", "Compaction", null, "Value");
        long memtableFlushPendingTasks = jmxRunner.getAttribute(connection, "org.apache.cassandra.internal", null, "MemtablePostFlusher", null, "PendingTasks");
        metrics.add(new Metric(String.format(COMPACTION_PENDING_TASKS, instance), "count", compactionPendingTasks));
        metrics.add(new Metric(String.format(MEMTABLE_PENDING_TASKS, instance), "count", memtableFlushPendingTasks));

        return metrics;
    }

    private List<Metric> getLatencyMetrics(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<Metric> metrics = new ArrayList<>();

        BigDecimal readMean = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest", "Read", "Mean");
        TimeUnit readMeanUnits = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest", "Read", "LatencyUnit");
        metrics.add(new Metric(String.format(READ_LATENCY_INSTANCE, instance), MILLIS, toMillis(readMean, readMeanUnits)));
        metrics.add(new Metric(READ_LATENCY_GLOBAL, MILLIS, toMillis(readMean, readMeanUnits)));

        BigDecimal writeMean = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest", "Write", "Mean");
        TimeUnit writeMeanUnits = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest", "Write", "LatencyUnit");
        metrics.add(new Metric(String.format(WRITE_LATENCY_INSTANCE, instance), MILLIS, toMillis(writeMean, writeMeanUnits)));
        metrics.add(new Metric(WRITE_LATENCY_GLOBAL, MILLIS, toMillis(writeMean, writeMeanUnits)));

        return metrics;
    }

    private BigDecimal toMillis(BigDecimal sourceValue, TimeUnit sourceUnit) {
        switch (sourceUnit) {
            case DAYS:
                return sourceValue.multiply(new BigDecimal(86400000));
            case MICROSECONDS:
                return sourceValue.multiply(new BigDecimal(0.001));
            case HOURS:
                return sourceValue.multiply(new BigDecimal(3600000));
            case MILLISECONDS:
                return sourceValue;
            case MINUTES:
                return sourceValue.multiply(new BigDecimal(60000));
            case NANOSECONDS:
                return sourceValue.multiply(new BigDecimal(1.0e-6));
            case SECONDS:
                return sourceValue.multiply(new BigDecimal(1000));
            default:
                return sourceValue;
        }
    }
}
