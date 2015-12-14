package com.fnklabs.draenei.orm.analytics;

import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.orm.DataProvider;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.ignite.configuration.CacheConfiguration;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class AnalyticsUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticsUtils.class);

    /**
     * Load data from cassandra into ignite
     * <p>
     * Will generate token range and execute tasks to load data from cassandra by token range
     *
     * @param dataProvider DataProvider from which will be loaded data
     * @param <ValueIn>    Entity class type
     *
     * @return Future for operation complete with number of loaded elements
     */
    @NotNull
    public static <ValueIn extends Serializable> ListenableFuture<Integer> loadData(@NotNull AnalyticsContext analyticsContext,
                                                                                    @NotNull DataProvider<ValueIn> dataProvider,
                                                                                    @NotNull CacheConfiguration<Long, ValueIn> cacheConfiguration) {


        CassandraClient cassandraClient = analyticsContext.getCassandraClientFactory().create();
        Collection<Range<Long>> ranges = CassandraUtils.splitRing(cassandraClient);

        List<ListenableFuture<Integer>> futures = new ArrayList<>();

        ExecutorService executorService = analyticsContext.getExecutorService();

        ranges.forEach(range -> {
            LoadDataTask<ValueIn> task = new LoadDataTask<>(range.lowerEndpoint(),
                    range.upperEndpoint(),
                    dataProvider.getEntityClass(),
                    analyticsContext.getCassandraClientFactory(),
                    cacheConfiguration);

            Future<Integer> responseFuture = executorService.submit(task);

            ListenableFuture<Integer> listenableFuture = JdkFutureAdapters.listenInPoolThread(responseFuture);

            futures.add(listenableFuture);

            Futures.addCallback(listenableFuture, new FutureCallback<Integer>() {
                @Override
                public void onSuccess(Integer result) {
                    LOGGER.info("Complete to load data: count {} in range {}", result, range);
                }

                @Override
                public void onFailure(Throwable t) {
                    LOGGER.warn("Can't fetch data", t);
                }
            });
        });


        return Futures.transform(Futures.allAsList(futures), (List<Integer> futuresResult) -> {
            Optional<Integer> reduceResult = futuresResult.stream().reduce(Integer::sum);

            return reduceResult.isPresent() ? reduceResult.get() : 0;
        });
    }

}
