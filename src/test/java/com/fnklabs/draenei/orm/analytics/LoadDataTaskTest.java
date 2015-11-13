package com.fnklabs.draenei.orm.analytics;

import com.fnklabs.draenei.orm.DataProvider;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoadDataTaskTest {

    @Test
    public void testCall() throws Exception {
        UUID jobId = UUID.randomUUID();
        LoadDataTask<? extends LoadDataTaskTest> loadDataTask = new LoadDataTask<>(0L, 100L, jobId, getClass());


        DataProvider dataProvider = mock(DataProvider.class);

        when(dataProvider.load(eq(0L), anyLong(), any(LoadIntoHazelcastConsumer.class))).thenReturn(1);
        when(dataProvider.load(eq(1L), anyLong(), any(LoadIntoHazelcastConsumer.class))).thenReturn(0);

        Analytics analytics = mock(Analytics.class);
        when(analytics.getDataProvider(any(Class.class))).thenReturn(dataProvider);

        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        IMap map = mock(IMap.class);
        when(hazelcastInstance.getMap(LoadDataTask.getMapName(jobId))).thenReturn(map);

        loadDataTask.setAnalyticsInstance(analytics);
        loadDataTask.setHazelcastInstance(hazelcastInstance);

        int call = loadDataTask.call();

        Assert.assertEquals(1, call);


        loadDataTask = new LoadDataTask<>(1, 100, jobId, getClass());
        loadDataTask.setAnalyticsInstance(analytics);
        loadDataTask.setHazelcastInstance(hazelcastInstance);

        call = loadDataTask.call();

        Assert.assertEquals(0, call);
    }
}