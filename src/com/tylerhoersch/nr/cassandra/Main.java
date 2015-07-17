package com.tylerhoersch.nr.cassandra;

import com.newrelic.metrics.publish.Runner;
import com.newrelic.metrics.publish.util.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            Runner runner = new Runner();
            runner.add(new CassandraAgentFactory());
            runner.setupAndRun();

        } catch (Exception e) {
            logger.error("ERROR: " + e.getMessage(), e);
            System.exit(0);
        }
    }
}
