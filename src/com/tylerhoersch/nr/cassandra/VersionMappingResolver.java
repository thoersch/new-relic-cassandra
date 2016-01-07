package com.tylerhoersch.nr.cassandra;

import com.tylerhoersch.nr.cassandra.templates.Cassandra2xMappings;

import java.util.HashMap;
import java.util.Map;

public class VersionMappingResolver {

    private final Map<Integer, AttributeJMXRequestMapper> mappers = new HashMap<Integer, AttributeJMXRequestMapper>() {{
        put(2, new Cassandra2xMappings());
    }};

    public AttributeJMXRequestMapper resolve(int version) {
        return mappers.get(version);
    }
}
