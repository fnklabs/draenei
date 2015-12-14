package com.fnklabs.draenei.orm.analytics;

import com.datastax.driver.core.Host;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.orm.CassandraClientFactory;
import com.fnklabs.draenei.orm.DataProvider;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnalyticsUtilsTest {

    @Test
    public void testLoadData() throws Exception {
        AnalyticsContext analyticsContext = mock(AnalyticsContext.class);
        CassandraClientFactory cassandraClientFactory = mock(CassandraClientFactory.class);

        CassandraClient cassandraClient = mock(CassandraClient.class);
        when(cassandraClientFactory.create()).thenReturn(cassandraClient);

        when(cassandraClient.getMembers()).thenReturn(Sets.newHashSet(mock(Host.class), mock(Host.class)));

        when(analyticsContext.getCassandraClientFactory()).thenReturn(cassandraClientFactory);


        ExecutorService executorService = mock(ExecutorService.class);
        when(executorService.submit(any(Callable.class))).thenReturn(Futures.immediateFuture(1));

        when(analyticsContext.getExecutorService()).thenReturn(executorService);

        ListenableFuture<Integer> loadedEntries = AnalyticsUtils.loadData(analyticsContext,
                mock(DataProvider.class),
                mock(CacheConfiguration.class));

        Assert.assertNotNull(loadedEntries);

        Assert.assertTrue(loadedEntries.get() > 1);
    }
}