package com.fnklabs.draenei.orm.analytics;

import com.fnklabs.draenei.orm.DataProvider;
import com.hazelcast.core.IMap;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.function.Consumer;


/**
 * Consumer that load data into hazelcast
 *
 * @param <T> Consumed data class type
 */
class LoadIntoHazelcastConsumer<T> implements Consumer<T>, Serializable {
    @NotNull
    private transient final IMap<Long, T> map;

    @NotNull
    private transient final DataProvider<T> dataProvider;

    LoadIntoHazelcastConsumer(@NotNull IMap<Long, T> map, @NotNull DataProvider<T> dataProvider) {
        this.map = map;
        this.dataProvider = dataProvider;
    }

    @Override
    public void accept(T t) {
        long key = dataProvider.buildCacheKey(t);
        map.put(key, t);
    }
}
