package com.fnklabs.draenei.orm.analytics;

import org.junit.Assert;
import org.junit.Test;

public class HazelcastReducerWrapperTest {

    private static final Reducer<Long, Long, Long> SUM_REDUCER = new Reducer<Long, Long, Long>() {
        @Override
        public Long reduce(Long aLong, Iterable<Long> longs) {
            long sum = 0;
            for (Long item : longs) {
                sum += item;
            }
            return sum;
        }


    };

    @Test
    public void testReduce() throws Exception {
        HazelcastReducerWrapper<Long, Long, Long> reducerWrapper = new HazelcastReducerWrapper<>(1L, SUM_REDUCER);
        reducerWrapper.reduce(1L);
    }

    @Test
    public void testFinalizeReduce() throws Exception {
        HazelcastReducerWrapper<Long, Long, Long> reducerWrapper = new HazelcastReducerWrapper<>(1L, SUM_REDUCER);
        reducerWrapper.reduce(1L);
        long sum = reducerWrapper.finalizeReduce();

        Assert.assertEquals(1L, sum);


        reducerWrapper.reduce(1L);
        reducerWrapper.reduce(1L);
        sum = reducerWrapper.finalizeReduce();

        Assert.assertEquals(3L, sum);
    }
}