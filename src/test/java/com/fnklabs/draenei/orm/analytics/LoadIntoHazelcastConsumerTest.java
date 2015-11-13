package com.fnklabs.draenei.orm.analytics;

import com.fnklabs.draenei.orm.DataProvider;
import com.hazelcast.core.IMap;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class LoadIntoHazelcastConsumerTest {

    @Test
    public void testAccept() throws Exception {
        DataProvider dataProvider = mock(DataProvider.class);
        IMap map = mock(IMap.class);

        when(dataProvider.buildCacheKey(anyLong())).thenReturn(1000L);

        LoadIntoHazelcastConsumer<Long> loadIntoHazelcastConsumer = new LoadIntoHazelcastConsumer<>(map, dataProvider);

        loadIntoHazelcastConsumer.accept(1L);

        verify(dataProvider, times(1)).buildCacheKey(new Long(1L));
        verify(map, times(1)).put(1000L, 1L);
    }
}