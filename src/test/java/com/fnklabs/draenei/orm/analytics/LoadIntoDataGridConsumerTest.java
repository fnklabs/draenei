package com.fnklabs.draenei.orm.analytics;

import com.fnklabs.draenei.orm.DataProvider;
import org.apache.ignite.IgniteCache;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class LoadIntoDataGridConsumerTest {

    @Test
    public void testAccept() throws Exception {
        DataProvider dataProvider = mock(DataProvider.class);
        IgniteCache map = mock(IgniteCache.class);

        when(dataProvider.buildHashCode(anyLong())).thenReturn(1000L);

        LoadDataTask.LoadIntoDataGridConsumer<Long> loadIntoDataGridConsumer = new LoadDataTask.LoadIntoDataGridConsumer<>(map, dataProvider);

        loadIntoDataGridConsumer.accept(1L);

        verify(dataProvider, times(1)).buildHashCode(new Long(1L));
        verify(map, times(1)).put(1000L, 1L);
    }
}