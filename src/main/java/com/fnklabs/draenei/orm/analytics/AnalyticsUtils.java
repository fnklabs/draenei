package com.fnklabs.draenei.orm.analytics;

import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.orm.CacheableDataProvider;
import com.fnklabs.draenei.orm.DataProvider;
import com.google.common.collect.Range;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class AnalyticsUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticsUtils.class);


    /**
     * Load data from cassandra into ignite
     * <p>
     * Will generate token range and execute tasks to load data from cassandra by token range
     *
     * @param analyticsContext Analytics context instance for retrieving distributed executor service and cassandra factory
     * @param dataProvider     DataProvider from which will be loaded data
     * @param <ValueIn>        Entity class type
     *
     * @return Future for operation complete with number of loaded elements
     */
    @NotNull
    public static <ValueIn extends Serializable> Integer boot(@NotNull AnalyticsContext analyticsContext,
                                                              @NotNull CacheableDataProvider<ValueIn> dataProvider) {
        CassandraClient cassandraClient = analyticsContext.getCassandraClientFactory().create();
        Collection<Range<Long>> ranges = CassandraUtils.splitRing(cassandraClient);

        List<BootDataTask<ValueIn>> calls = ranges.stream()
                                                      .map(range -> {
                                                          BootDataTask<ValueIn> serializableSeekOverDataTask = new BootDataTask<>(
                                                                  range.lowerEndpoint(),
                                                                  range.upperEndpoint(),
                                                                  dataProvider.getEntityClass(),
                                                                  analyticsContext.getCassandraClientFactory()
                                                          );

                                                          return serializableSeekOverDataTask;
                                                      })
                                                      .collect(Collectors.toList());

        ClusterGroup clusterGroup = analyticsContext.getIgnite()
                                                    .cluster()
                                                    .forServers();

        Integer loadedDocuments = analyticsContext.getIgnite()
                                                  .compute(clusterGroup)
                                                  .call(calls, new ResultReducer());


        return loadedDocuments;
    }

    /**
     * Load data from cassandra into ignite
     * <p>
     * Will generate token range and execute tasks to load data from cassandra by token range
     *
     * @param analyticsContext Analytics context instance for retrieving distributed executor service and cassandra factory
     * @param dataProvider     DataProvider from which will be loaded data
     * @param <ValueIn>        Entity class type
     *
     * @return Future for operation complete with number of loaded elements
     */
    @NotNull
    public static <ValueIn extends Serializable, UserCallback extends Consumer<ValueIn> & Serializable> Integer map(@NotNull AnalyticsContext analyticsContext,
                                                                                                                    @NotNull DataProvider<ValueIn> dataProvider,
                                                                                                                    @NotNull UserCallback mapConsumer) {
        CassandraClient cassandraClient = analyticsContext.getCassandraClientFactory().create();
        Collection<Range<Long>> ranges = CassandraUtils.splitRing(cassandraClient);

        List<SeekOverDataTask<ValueIn, UserCallback>> calls = ranges.stream()
                                                                    .map(range -> {
                                                                        return new SeekOverDataTask<>(
                                                                                range.lowerEndpoint(),
                                                                                range.upperEndpoint(),
                                                                                dataProvider.getEntityClass(),
                                                                                analyticsContext.getCassandraClientFactory(),
                                                                                mapConsumer);
                                                                    })
                                                                    .collect(Collectors.toList());

        ClusterGroup clusterGroup = analyticsContext.getIgnite()
                                                    .cluster()
                                                    .forServers();

        Integer loadedDocuments = analyticsContext.getIgnite()
                                                  .compute(clusterGroup)
                                                  .call(calls, new ResultReducer());


        return loadedDocuments;
    }

    /**
     * Execute compute operation with MR paradigm
     *
     * @param analyticsContext   Analytics context instance for retrieving distributed executor service and cassandra factory
     * @param cacheConfiguration Cache
     * @param <ValueIn>          Entity class type
     *
     * @return Reduce result
     */
    @NotNull
    public static <Key extends Serializable, ValueIn, MapOutput extends Serializable, ReducerResult extends Serializable> ReducerResult compute(@NotNull AnalyticsContext analyticsContext,
                                                                                                                                                @NotNull CacheConfiguration<Key, ValueIn> cacheConfiguration,
                                                                                                                                                @NotNull MapFunction<Key, ValueIn, MapOutput> mapFunction,
                                                                                                                                                @NotNull ReduceFunction<MapOutput, ReducerResult> reduceFunction) {
        ClusterGroup clusterGroup = analyticsContext.getIgnite()
                                                    .cluster()
                                                    .forServers();

        TaskAdapter<Key, ValueIn, Object, MapOutput, ReducerResult> task = new TaskAdapter<>(mapFunction, reduceFunction, cacheConfiguration);

        ReducerResult execute = analyticsContext.getIgnite()
                                                .compute(clusterGroup)
                                                .execute(task, null);

        return execute;
    }
}
