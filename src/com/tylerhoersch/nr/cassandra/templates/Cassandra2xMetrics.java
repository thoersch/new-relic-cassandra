package com.tylerhoersch.nr.cassandra.templates;

import com.newrelic.metrics.publish.util.Logger;
import com.tylerhoersch.nr.cassandra.JMXRunner;
import com.tylerhoersch.nr.cassandra.JMXTemplate;
import com.tylerhoersch.nr.cassandra.Metric;

import javax.management.MBeanServerConnection;
import java.lang.Long;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Cassandra2xMetrics implements JMXTemplate<List<Metric>> {

    private static final Logger logger = Logger.getLogger(Cassandra2xMetrics.class);

    private static final String READ_LATENCY_INSTANCE = "Cassandra/hosts/%s/Latency/Reads";
    private static final String WRITE_LATENCY_INSTANCE = "Cassandra/hosts/%s/Latency/Writes";
    private static final String READ_LATENCY_GLOBAL = "Cassandra/global/Latency/Reads";
    private static final String WRITE_LATENCY_GLOBAL = "Cassandra/global/Latency/Writes";
    private static final String READ_TIMEOUTS = "Cassandra/hosts/%s/Timeouts/Reads";
    private static final String WRITE_TIMEOUTS = "Cassandra/hosts/%s/Timeouts/Writes";

    private static final String COMPACTION_PENDING_TASKS = "Cassandra/hosts/%s/Compaction/PendingTasks";
    private static final String MEMTABLE_PENDING_TASKS = "Cassandra/hosts/%s/MemtableFlush/PendingTasks";

    private static final String STORAGE_LOAD_INSTANCE = "Cassandra/host/%s/Storage/Data";
    private static final String STORAGE_LOAD_GLOBAL = "Cassandra/global/Storage/Data";
    private static final String COMMIT_LOG_INSTANCE = "Cassandra/host/%s/Storage/CommitLog";
    private static final String COMMIT_LOG_GLOBAL = "Cassandra/global/Storage/CommitLog";

    private static final String KEY_CACHE_HIT_RATE_INSTANCE = "Cassandra/hosts/%s/Cache/KeyCache/HitRate";
    private static final String KEY_CACHE_HIT_RATE_GLOBAL = "Cassandra/global/Cache/KeyCache/HitRate";
    private static final String KEY_CACHE_SIZE_INSTANCE = "Cassandra/hosts/%s/Cache/KeyCache/Size";
    private static final String KEY_CACHE_SIZE_GLOBAL = "Cassandra/global/Cache/KeyCache/Size";
    private static final String KEY_CACHE_ENTRIES_INSTANCE = "Cassandra/hosts/%s/Cache/KeyCache/Entries";
    private static final String KEY_CACHE_ENTRIES_GLOBAL = "Cassandra/global/Cache/KeyCache/Entries";
    private static final String ROW_CACHE_HIT_RATE_INSTANCE = "Cassandra/hosts/%s/Cache/RowCache/HitRate";
    private static final String ROW_CACHE_HIT_RATE_GLOBAL = "Cassandra/global/Cache/RowCache/HitRate";
    private static final String ROW_CACHE_SIZE_INSTANCE = "Cassandra/hosts/%s/Cache/RowCache/Size";
    private static final String ROW_CACHE_SIZE_GLOBAL = "Cassandra/global/Cache/RowCache/Size";
    private static final String ROW_CACHE_ENTRIES_INSTANCE = "Cassandra/hosts/%s/Cache/RowCache/Entries";
    private static final String ROW_CACHE_ENTRIES_GLOBAL = "Cassandra/global/Cache/RowCache/Entries";

    private static final String MILLIS = "millis";
    private static final String RATE = "rate";
    private static final String BYTES = "bytes";
    private static final String COUNT = "count";

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
        metrics.addAll(getCacheMetrics(connection, jmxRunner));

        return metrics;
    }

    private List<Metric> getCacheMetrics(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<Metric> metrics = new ArrayList<>();

        try {
            Double keyCacheHitRate = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "HitRate", "Cache", "KeyCache", "Value");
            metrics.add(new Metric(String.format(KEY_CACHE_HIT_RATE_INSTANCE, instance), RATE, keyCacheHitRate));
            metrics.add(new Metric(KEY_CACHE_HIT_RATE_GLOBAL, RATE, keyCacheHitRate));

            Long keyCacheSize = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "Size", "Cache", "KeyCache", "Value");
            metrics.add(new Metric(String.format(KEY_CACHE_SIZE_INSTANCE, instance), BYTES, keyCacheSize));
            metrics.add(new Metric(KEY_CACHE_SIZE_GLOBAL, BYTES, keyCacheSize));

            Integer keyCacheEntries = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "Entries", "Cache", "KeyCache", "Value");
            metrics.add(new Metric(String.format(KEY_CACHE_ENTRIES_INSTANCE, instance), COUNT, keyCacheEntries));
            metrics.add(new Metric(KEY_CACHE_ENTRIES_GLOBAL, COUNT, keyCacheEntries));

            Double rowCacheHitRate = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "HitRate", "Cache", "RowCache", "Value");
            metrics.add(new Metric(String.format(ROW_CACHE_HIT_RATE_INSTANCE, instance), RATE, rowCacheHitRate));
            metrics.add(new Metric(ROW_CACHE_HIT_RATE_GLOBAL, RATE, rowCacheHitRate));

            Long rowCacheSize = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "Size", "Cache", "RowCache", "Value");
            metrics.add(new Metric(String.format(ROW_CACHE_SIZE_INSTANCE, instance), BYTES, rowCacheSize));
            metrics.add(new Metric(ROW_CACHE_SIZE_GLOBAL, BYTES, rowCacheSize));

            Integer rowCacheEntries = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "Entries", "Cache", "RowCache", "Value");
            metrics.add(new Metric(String.format(ROW_CACHE_ENTRIES_INSTANCE, instance), COUNT, rowCacheEntries));
            metrics.add(new Metric(ROW_CACHE_ENTRIES_GLOBAL, COUNT, rowCacheEntries));
        } catch(Exception e) {
            logger.error(e);
        }

        return metrics;
    }

    private List<Metric> getStorageMetrics(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<Metric> metrics = new ArrayList<>();

        try {
            Double load = jmxRunner.getAttribute(connection, "org.apache.cassandra.db", null, "StorageService", null, "Load");
            metrics.add(new Metric(String.format(STORAGE_LOAD_INSTANCE, instance), BYTES, load));
            metrics.add(new Metric(STORAGE_LOAD_GLOBAL, BYTES, load));

            Long commitLogSize = jmxRunner.getAttribute(connection, "org.apache.cassandra.db", null, "Commitlog", null, "TotalCommitlogSize");
            metrics.add(new Metric(String.format(COMMIT_LOG_INSTANCE, instance), BYTES, commitLogSize));
            metrics.add(new Metric(COMMIT_LOG_GLOBAL, BYTES, commitLogSize));
        } catch (Exception e) {
            logger.error(e);
        }

        return metrics;
    }

    private List<Metric> getSystemMetrics(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<Metric> metrics = new ArrayList<>();

        try {
            Integer compactionPendingTasks = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "PendingTasks", "Compaction", null, "Value");
            Long memtableFlushPendingTasks = jmxRunner.getAttribute(connection, "org.apache.cassandra.internal", null, "MemtablePostFlusher", null, "PendingTasks");
            metrics.add(new Metric(String.format(COMPACTION_PENDING_TASKS, instance), COUNT, compactionPendingTasks));
            metrics.add(new Metric(String.format(MEMTABLE_PENDING_TASKS, instance), COUNT, memtableFlushPendingTasks));
        } catch (Exception e) {
            logger.error(e);
        }

        return metrics;
    }

    private List<Metric> getLatencyMetrics(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<Metric> metrics = new ArrayList<>();

        try {
            Double readMean = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest", "Read", "Mean");
            TimeUnit readMeanUnits = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest", "Read", "LatencyUnit");
            metrics.add(new Metric(String.format(READ_LATENCY_INSTANCE, instance), MILLIS, toMillis(readMean, readMeanUnits)));
            metrics.add(new Metric(READ_LATENCY_GLOBAL, MILLIS, toMillis(readMean, readMeanUnits)));

            Double writeMean = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest", "Write", "Mean");
            TimeUnit writeMeanUnits = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest", "Write", "LatencyUnit");
            metrics.add(new Metric(String.format(WRITE_LATENCY_INSTANCE, instance), MILLIS, toMillis(writeMean, writeMeanUnits)));
            metrics.add(new Metric(WRITE_LATENCY_GLOBAL, MILLIS, toMillis(writeMean, writeMeanUnits)));

            Long readTimeouts = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "Timeouts", "ClientRequest", "Read", "Count");
            metrics.add(new Metric(String.format(READ_TIMEOUTS, instance), COUNT, readTimeouts));

            Long writeTimeouts = jmxRunner.getAttribute(connection, "org.apache.cassandra.metrics", "Timeouts", "ClientRequest", "Write", "Count");
            metrics.add(new Metric(String.format(WRITE_TIMEOUTS, instance), COUNT, writeTimeouts));
        } catch(Exception e) {
            logger.error(e);
        }

        return metrics;
    }

    private Double toMillis(Double sourceValue, TimeUnit sourceUnit) {
        switch (sourceUnit) {
            case DAYS:
                return sourceValue * 86400000;
            case MICROSECONDS:
                return sourceValue * 0.001;
            case HOURS:
                return sourceValue * 3600000;
            case MILLISECONDS:
                return sourceValue;
            case MINUTES:
                return sourceValue * 60000;
            case NANOSECONDS:
                return sourceValue * 1.0e-6;
            case SECONDS:
                return sourceValue * 1000;
            default:
                return sourceValue;
        }
    }
}