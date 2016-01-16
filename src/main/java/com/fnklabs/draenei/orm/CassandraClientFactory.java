package com.fnklabs.draenei.orm;

import com.fnklabs.draenei.CassandraClient;

import java.io.Serializable;

/**
 * Factory that must create and return instance of {@link CassandraClient}
 */
public interface CassandraClientFactory extends Serializable {
    CassandraClient create();
}
