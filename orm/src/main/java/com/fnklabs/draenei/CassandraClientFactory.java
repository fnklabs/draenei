package com.fnklabs.draenei;

import javax.cache.configuration.Factory;

public class CassandraClientFactory implements Factory<CassandraClient> {
    private static transient CassandraClient CASSANDRA_CLIENT;
    private final String username;
    private final String password;
    private final String defaultKeyspace;
    private final String hosts;

    public CassandraClientFactory(String username, String password, String defaultKeyspace, String hosts) {
        this.username = username;
        this.password = password;
        this.defaultKeyspace = defaultKeyspace;
        this.hosts = hosts;
    }

    @Override
    public synchronized CassandraClient create() {
        if (CASSANDRA_CLIENT == null) {
            CASSANDRA_CLIENT = new CassandraClient(username, password, defaultKeyspace, hosts);
        }

        return CASSANDRA_CLIENT;
    }
}
