package com.fnklabs.draenei.orm.analytics;

import com.hazelcast.mapreduce.Context;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class HazelcastContextWrapperTest {

    @Test
    public void testEmit() throws Exception {
        Context context = mock(Context.class);
        HazelcastContextWrapper<Long, Long> contextWrapper = new HazelcastContextWrapper<>(context);
        contextWrapper.emit(1L, 1L);

        verify(context, times(1)).emit(1L, 1L);
        verify(context, never()).emit(0, 1L);
    }
}