package com.fnklabs.draenei.orm;

import com.fnklabs.draenei.CassandraClient;

/**
 * Factory that must create and return instance of {@link CassandraClient}
 */
public interface CassandraClientFactory {
    CassandraClient create();
}
