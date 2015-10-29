package com.fnklabs.draenei.analytics;

import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.MetricsFactory;
import com.fnklabs.draenei.orm.CacheableDataProvider;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import tv.nemo.content.entity.StopWord;

@Service
public class StopWordsDao extends CacheableDataProvider<StopWord> {

    @Autowired
    public StopWordsDao(CassandraClient cassandraClient,
                        HazelcastInstance hazelcastInstance,
                        MetricsFactory metricsFactory,
                        @Qualifier(value = "dataProviderExecutorService") ListeningExecutorService executorService) {
        super(StopWord.class, cassandraClient, hazelcastInstance, metricsFactory, executorService);
    }


}
