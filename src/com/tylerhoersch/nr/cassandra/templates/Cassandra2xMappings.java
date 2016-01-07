package com.tylerhoersch.nr.cassandra.templates;

import com.tylerhoersch.nr.cassandra.AttributeJMXRequestMapper;
import com.tylerhoersch.nr.cassandra.AttributeType;
import com.tylerhoersch.nr.cassandra.JMXRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Cassandra2xMappings implements AttributeJMXRequestMapper {
    private final Logger logger = LoggerFactory.getLogger(Cassandra2xMappings.class);
    private final int VERSION = 2;

    private final Map<AttributeType, JMXRequest> attributeMap = new HashMap<AttributeType, JMXRequest>() {{
        put(AttributeType.KEY_CACHE_HIT_RATE, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("HitRate").type("Cache").scope("KeyCache").attribute("Value").build());
        put(AttributeType.KEY_CACHE_SIZE, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("Size").type("Cache").scope("KeyCache").attribute("Value").build());
        put(AttributeType.KEY_CACHE_ENTRIES, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("Entries").type("Cache").scope("KeyCache").attribute("Value").build());
        put(AttributeType.KEY_CACHE_REQUESTS, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("Requests").type("Cache").scope("KeyCache").attribute("OneMinuteRate").build());
        put(AttributeType.ROW_CACHE_HIT_RATE, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("HitRate").type("Cache").scope("RowCache").attribute("Value").build());
        put(AttributeType.ROW_CACHE_SIZE, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("Size").type("Cache").scope("RowCache").attribute("Value").build());
        put(AttributeType.ROW_CACHE_ENTRIES, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("Entries").type("Cache").scope("RowCache").attribute("Value").build());
        put(AttributeType.ROW_CACHE_REQUESTS, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("Requests").type("Cache").scope("RowCache").attribute("OneMinuteRate").build());
        put(AttributeType.STORAGE_LOAD, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.db").type("StorageService").attribute("LoadString").build());
        put(AttributeType.COMMIT_LOG, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.db").type("Commitlog").attribute("TotalCommitlogSize").build());
        put(AttributeType.COMPACTION_PENDING_TASKS_VALUE, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("PendingTasks").type("Compaction").attribute("Value").build());
        put(AttributeType.MEMTABLE_PENDING_TASKS_COUNT, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.internal").type("MemtablePostFlusher").attribute("PendingTasks").build());
        put(AttributeType.READ_LATENCY_MEAN, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("Latency").type("ClientRequest").scope("Read").attribute("Mean").build());
        put(AttributeType.READ_LATENCY_UNIT, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("Latency").type("ClientRequest").scope("Read").attribute("LatencyUnit").build());
        put(AttributeType.WRITE_LATENCY_MEAN, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("Latency").type("ClientRequest").scope("Write").attribute("Mean").build());
        put(AttributeType.WRITE_LATENCY_UNIT, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("Latency").type("ClientRequest").scope("Write").attribute("LatencyUnit").build());
        put(AttributeType.READ_TIMEOUTS_COUNT, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("Timeouts").type("ClientRequest").scope("Read").attribute("Count").build());
        put(AttributeType.WRITE_TIMEOUTS_COUNT, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("Timeouts").type("ClientRequest").scope("Write").attribute("Count").build());
        put(AttributeType.READ_LATENCY_TOTAL_COUNT, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("TotalLatency").type("ClientRequest").scope("Read").attribute("Count").build());
        put(AttributeType.WRITE_LATENCY_TOTAL_COUNT, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("TotalLatency").type("ClientRequest").scope("Write").attribute("Count").build());
        put(AttributeType.READ_UNAVAILABLE_REQUESTS, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("Unavailables").type("ClientRequest").scope("Read").attribute("MeanRate").build());
        put(AttributeType.READ_UNAVAILABLE_REQUESTS_UNIT, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("Unavailables").type("ClientRequest").scope("Read").attribute("RateUnit").build());
        put(AttributeType.WRITE_UNAVAILABLE_REQUESTS, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("Unavailables").type("ClientRequest").scope("Write").attribute("MeanRate").build());
        put(AttributeType.WRITE_UNAVAILABLE_REQUESTS_UNIT, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.metrics").name("Unavailables").type("ClientRequest").scope("Write").attribute("RateUnit").build());
        put(AttributeType.HOST_STATES, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.net").type("FailureDetector").attribute("SimpleStates").build());
        put(AttributeType.HOST_DOWN_COUNT, new JMXRequest.JMXRequestBuilder("org.apache.cassandra.net").type("FailureDetector").attribute("DownEndpointCount").build());
    }};

    @Override
    public boolean canHandleVersion(int version) {
        return version == VERSION;
    }

    @Override
    public JMXRequest get(AttributeType type) {
        JMXRequest request = null;

        if(attributeMap.containsKey(type)) {
            request = attributeMap.get(type);
        } else {
            logger.error(String.format("Could not find attribute mapping. Version=%d Attribute=%s", VERSION, type.toString()));
            throw new NullPointerException("Attribute Type not found in mapping: " + type.toString());
        }

        return request;
    }
}
