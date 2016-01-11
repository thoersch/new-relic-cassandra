package com.tylerhoersch.nr.cassandra.templates;

import com.newrelic.metrics.publish.util.Logger;
import com.tylerhoersch.nr.cassandra.*;
import com.tylerhoersch.nr.cassandra.utility.FormattedSizeUtility;
import com.tylerhoersch.nr.cassandra.utility.TimeUnitUtility;

import javax.management.MBeanServerConnection;
import java.lang.Long;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CassandraMetrics implements JMXTemplate<List<Metric>> {

    private static final Logger logger = Logger.getLogger(Cassandra2xMappings.class);

    public static final String READ_LATENCY_INSTANCE = "Cassandra/hosts/%s/Latency/Reads";
    public static final String WRITE_LATENCY_INSTANCE = "Cassandra/hosts/%s/Latency/Writes";
    public static final String READ_LATENCY_GLOBAL = "Cassandra/global/Latency/Reads";
    public static final String WRITE_LATENCY_GLOBAL = "Cassandra/global/Latency/Writes";
    public static final String READ_TIMEOUTS = "Cassandra/hosts/%s/Timeouts/Reads";
    public static final String WRITE_TIMEOUTS = "Cassandra/hosts/%s/Timeouts/Writes";
    public static final String READ_LATENCY_TOTAL_INSTANCE = "Cassandra/hosts/%s/LatencyTotal/Reads";
    public static final String WRITE_LATENCY_TOTAL_INSTANCE = "Cassandra/hosts/%s/LatencyTotal/Writes";
    public static final String READ_UNAVAILABLE_REQUESTS_INSTANCE = "Cassandra/hosts/%s/Unavailables/Reads";
    public static final String WRITE_UNAVAILABLE_REQUESTS_INSTANCE = "Cassandra/hosts/%s/Unavailables/Writes";

    public static final String COMPACTION_PENDING_TASKS = "Cassandra/hosts/%s/Compaction/PendingTasks";
    public static final String MEMTABLE_PENDING_TASKS = "Cassandra/hosts/%s/MemtableFlush/PendingTasks";

    public static final String STORAGE_LOAD_INSTANCE = "Cassandra/hosts/%s/Storage/Data";
    public static final String STORAGE_LOAD_GLOBAL = "Cassandra/global/Storage/Data";
    public static final String COMMIT_LOG_INSTANCE = "Cassandra/hosts/%s/Storage/CommitLog";
    public static final String COMMIT_LOG_GLOBAL = "Cassandra/global/Storage/CommitLog";

    public static final String KEY_CACHE_HIT_RATE_INSTANCE = "Cassandra/hosts/%s/Cache/KeyCache/HitRate";
    public static final String KEY_CACHE_HIT_RATE_GLOBAL = "Cassandra/global/Cache/KeyCache/HitRate";
    public static final String KEY_CACHE_SIZE_INSTANCE = "Cassandra/hosts/%s/Cache/KeyCache/Size";
    public static final String KEY_CACHE_SIZE_GLOBAL = "Cassandra/global/Cache/KeyCache/Size";
    public static final String KEY_CACHE_ENTRIES_INSTANCE = "Cassandra/hosts/%s/Cache/KeyCache/Entries";
    public static final String KEY_CACHE_ENTRIES_GLOBAL = "Cassandra/global/Cache/KeyCache/Entries";
    public static final String KEY_CACHE_REQUESTS_INSTANCE = "Cassandra/hosts/%s/Cache/KeyCache/Requests";
    public static final String ROW_CACHE_HIT_RATE_INSTANCE = "Cassandra/hosts/%s/Cache/RowCache/HitRate";
    public static final String ROW_CACHE_HIT_RATE_GLOBAL = "Cassandra/global/Cache/RowCache/HitRate";
    public static final String ROW_CACHE_SIZE_INSTANCE = "Cassandra/hosts/%s/Cache/RowCache/Size";
    public static final String ROW_CACHE_SIZE_GLOBAL = "Cassandra/global/Cache/RowCache/Size";
    public static final String ROW_CACHE_ENTRIES_INSTANCE = "Cassandra/hosts/%s/Cache/RowCache/Entries";
    public static final String ROW_CACHE_ENTRIES_GLOBAL = "Cassandra/global/Cache/RowCache/Entries";
    public static final String ROW_CACHE_REQUESTS_INSTANCE = "Cassandra/hosts/%s/Cache/RowCache/Requests";

    private static final String MILLIS = "millis";
    private static final String RATE = "rate";
    private static final String BYTES = "bytes";
    private static final String COUNT = "count";

    private final String instance;
    private final AttributeJMXRequestMapper attributeMap;

    public CassandraMetrics(String instance, AttributeJMXRequestMapper attributeMap) {
        this.instance = instance;
        this.attributeMap = attributeMap;
    }

    @Override
    public List<Metric> execute(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<Metric> metrics = new ArrayList<>();

        try {
            metrics.addAll(getLatencyMetrics(connection, jmxRunner));
            metrics.addAll(getSystemMetrics(connection, jmxRunner));
            metrics.addAll(getStorageMetrics(connection, jmxRunner));
            metrics.addAll(getCacheMetrics(connection, jmxRunner));
        } catch (Exception e) {
            logger.error("Error polling for metrics:", e);
        }

        return metrics;
    }

    private List<Metric> getCacheMetrics(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<Metric> metrics = new ArrayList<>();

        Double keyCacheHitRate = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.KEY_CACHE_HIT_RATE));
        metrics.add(new Metric(String.format(KEY_CACHE_HIT_RATE_INSTANCE, instance), RATE, keyCacheHitRate));
        metrics.add(new Metric(KEY_CACHE_HIT_RATE_GLOBAL, RATE, keyCacheHitRate));

        Long keyCacheSize = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.KEY_CACHE_SIZE));
        metrics.add(new Metric(String.format(KEY_CACHE_SIZE_INSTANCE, instance), BYTES, keyCacheSize));
        metrics.add(new Metric(KEY_CACHE_SIZE_GLOBAL, BYTES, keyCacheSize));

        Integer keyCacheEntries = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.KEY_CACHE_ENTRIES));
        metrics.add(new Metric(String.format(KEY_CACHE_ENTRIES_INSTANCE, instance), COUNT, keyCacheEntries));
        metrics.add(new Metric(KEY_CACHE_ENTRIES_GLOBAL, COUNT, keyCacheEntries));

        Double keyCacheRequestsRate = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.KEY_CACHE_REQUESTS));
        metrics.add(new Metric(String.format(KEY_CACHE_REQUESTS_INSTANCE, instance), RATE, keyCacheRequestsRate));

        Double rowCacheHitRate = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.ROW_CACHE_HIT_RATE));
        metrics.add(new Metric(String.format(ROW_CACHE_HIT_RATE_INSTANCE, instance), RATE, rowCacheHitRate));
        metrics.add(new Metric(ROW_CACHE_HIT_RATE_GLOBAL, RATE, rowCacheHitRate));

        Long rowCacheSize = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.ROW_CACHE_SIZE));
        metrics.add(new Metric(String.format(ROW_CACHE_SIZE_INSTANCE, instance), BYTES, rowCacheSize));
        metrics.add(new Metric(ROW_CACHE_SIZE_GLOBAL, BYTES, rowCacheSize));

        Integer rowCacheEntries = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.ROW_CACHE_ENTRIES));
        metrics.add(new Metric(String.format(ROW_CACHE_ENTRIES_INSTANCE, instance), COUNT, rowCacheEntries));
        metrics.add(new Metric(ROW_CACHE_ENTRIES_GLOBAL, COUNT, rowCacheEntries));

        Double rowCacheRequestsRate = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.ROW_CACHE_REQUESTS));
        metrics.add(new Metric(String.format(ROW_CACHE_REQUESTS_INSTANCE, instance), RATE, rowCacheRequestsRate));

        return metrics;
    }

    private List<Metric> getStorageMetrics(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<Metric> metrics = new ArrayList<>();

        String loadString = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.STORAGE_LOAD));
        Double load = FormattedSizeUtility.parse(loadString);
        metrics.add(new Metric(String.format(STORAGE_LOAD_INSTANCE, instance), BYTES, load));
        metrics.add(new Metric(STORAGE_LOAD_GLOBAL, BYTES, load));

        Long commitLogSize = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.COMMIT_LOG));
        metrics.add(new Metric(String.format(COMMIT_LOG_INSTANCE, instance), BYTES, commitLogSize));
        metrics.add(new Metric(COMMIT_LOG_GLOBAL, BYTES, commitLogSize));

        return metrics;
    }

    private List<Metric> getSystemMetrics(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<Metric> metrics = new ArrayList<>();

        Integer compactionPendingTasks = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.COMPACTION_PENDING_TASKS_VALUE));
        Long memtableFlushPendingTasks = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.MEMTABLE_PENDING_TASKS_COUNT));
        metrics.add(new Metric(String.format(COMPACTION_PENDING_TASKS, instance), COUNT, compactionPendingTasks));
        metrics.add(new Metric(String.format(MEMTABLE_PENDING_TASKS, instance), COUNT, memtableFlushPendingTasks));

        return metrics;
    }

    private List<Metric> getLatencyMetrics(MBeanServerConnection connection, JMXRunner jmxRunner) throws Exception {
        List<Metric> metrics = new ArrayList<>();

        Double readMean = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.READ_LATENCY_MEAN));
        TimeUnit readMeanUnits = TimeUnitUtility.getTimeUnit(jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.READ_LATENCY_UNIT)));
        metrics.add(new Metric(String.format(READ_LATENCY_INSTANCE, instance), MILLIS, TimeUnitUtility.toMillis(readMean, readMeanUnits)));
        metrics.add(new Metric(READ_LATENCY_GLOBAL, MILLIS, TimeUnitUtility.toMillis(readMean, readMeanUnits)));

        Double writeMean = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.WRITE_LATENCY_MEAN));
        TimeUnit writeMeanUnits = TimeUnitUtility.getTimeUnit(jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.WRITE_LATENCY_UNIT)));
        metrics.add(new Metric(String.format(WRITE_LATENCY_INSTANCE, instance), MILLIS, TimeUnitUtility.toMillis(writeMean, writeMeanUnits)));
        metrics.add(new Metric(WRITE_LATENCY_GLOBAL, MILLIS, TimeUnitUtility.toMillis(writeMean, writeMeanUnits)));

        Long readTimeouts = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.READ_TIMEOUTS_COUNT));
        metrics.add(new Metric(String.format(READ_TIMEOUTS, instance), COUNT, readTimeouts));

        Long writeTimeouts = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.WRITE_TIMEOUTS_COUNT));
        metrics.add(new Metric(String.format(WRITE_TIMEOUTS, instance), COUNT, writeTimeouts));

        Long totalReadLatency = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.READ_LATENCY_TOTAL_COUNT));
        metrics.add(new Metric(String.format(READ_LATENCY_TOTAL_INSTANCE, instance), MILLIS, totalReadLatency * 0.001));

        Long totalWriteLatency = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.WRITE_LATENCY_TOTAL_COUNT));
        metrics.add(new Metric(String.format(WRITE_LATENCY_TOTAL_INSTANCE, instance), MILLIS, totalWriteLatency * 0.001));

        Double readUnavailableRequests = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.READ_UNAVAILABLE_REQUESTS));
        TimeUnit readUnavailableRequestsUnits = TimeUnitUtility.getTimeUnit(jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.READ_UNAVAILABLE_REQUESTS_UNIT)));
        metrics.add(new Metric(String.format(READ_UNAVAILABLE_REQUESTS_INSTANCE, instance), MILLIS, TimeUnitUtility.toMillis(readUnavailableRequests, readUnavailableRequestsUnits)));

        Double writeUnavailableRequests = jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.WRITE_UNAVAILABLE_REQUESTS));

        TimeUnit writeUnavailableRequestsUnits = TimeUnitUtility.getTimeUnit(jmxRunner.getAttribute(connection, attributeMap.get(AttributeType.WRITE_UNAVAILABLE_REQUESTS_UNIT)));
        metrics.add(new Metric(String.format(WRITE_UNAVAILABLE_REQUESTS_INSTANCE, instance), MILLIS, TimeUnitUtility.toMillis(writeUnavailableRequests, writeUnavailableRequestsUnits)));

        return metrics;
    }
}
