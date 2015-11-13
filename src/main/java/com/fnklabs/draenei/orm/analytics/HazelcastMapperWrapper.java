package com.fnklabs.draenei.orm.analytics;

import com.hazelcast.mapreduce.*;

class HazelcastMapperWrapper<ValueIn, KeyOut, ValueOut> implements com.hazelcast.mapreduce.Mapper<Long, ValueIn, KeyOut, ValueOut> {
    private final Mapper<ValueIn, KeyOut, ValueOut> mapper;

    public HazelcastMapperWrapper(Mapper<ValueIn, KeyOut, ValueOut> mapper) {
        this.mapper = mapper;
    }

    @Override
    public void map(Long key, ValueIn value, com.hazelcast.mapreduce.Context<KeyOut, ValueOut> context) {
        mapper.map(key, value, new HazelcastContextWrapper<KeyOut, ValueOut>(context));
    }
}