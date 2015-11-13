package com.fnklabs.draenei.analytics;

import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.MetricsFactory;
import com.fnklabs.draenei.orm.CacheableDataProvider;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;

public abstract class StopWordsDao extends CacheableDataProvider<StopWord> {
    public StopWordsDao(CassandraClient cassandraClient,
                        HazelcastInstance hazelcastInstance,
                        MetricsFactory metricsFactory,
                        ListeningExecutorService executorService) {
        super(StopWord.class, cassandraClient, hazelcastInstance, metricsFactory, executorService);
    }
}
