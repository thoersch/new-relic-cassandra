package com.tylerhoersch.nr.cassandra;

import java.util.ArrayList;
import java.util.List;

public class JMXRunnerFactory {

    public JMXRunner createJMXRunner(String host, String port, String username, String password) {
        return createJMXRunner(new ArrayList<String>() {{
            add(host);
        }}, port, username, password);
    }

    public JMXRunner createJMXRunner(List<String> hosts, String port, String username, String password) {
        return new JMXRunner(hosts, port, username, password);
    }
}
