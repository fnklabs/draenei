package com.fnklabs.draenei.analytics;

import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.MetricsFactory;
import com.fnklabs.draenei.orm.CacheableDataProvider;
import com.google.common.util.concurrent.ListeningExecutorService;

public abstract class StopWordsDao extends CacheableDataProvider<StopWord> {
    public StopWordsDao(CassandraClient cassandraClient,
                        MetricsFactory metricsFactory,
                        ListeningExecutorService executorService) {
        // todo change it
        super(StopWord.class, null, null,null, metricsFactory);
    }
}
