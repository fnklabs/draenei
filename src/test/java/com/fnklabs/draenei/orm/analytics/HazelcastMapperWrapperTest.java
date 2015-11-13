package com.fnklabs.draenei.orm.analytics;

import com.hazelcast.mapreduce.Context;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class HazelcastMapperWrapperTest {

    @Test
    public void testMap() throws Exception {
        Mapper mock = mock(Mapper.class);

        HazelcastMapperWrapper<Long, Long, Long> mapperWrapper = new HazelcastMapperWrapper<>(mock);
        mapperWrapper.map(1L, 2L, mock(Context.class));

        verify(mock, times(1)).map(eq(1L), eq(2L), any(com.fnklabs.draenei.orm.analytics.Context.class));
        verify(mock, never()).map(eq(0L), eq(2L), any(com.fnklabs.draenei.orm.analytics.Context.class));

    }
}