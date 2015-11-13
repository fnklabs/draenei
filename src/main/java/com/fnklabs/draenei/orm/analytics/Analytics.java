package com.fnklabs.draenei.orm.analytics;

import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.MetricsFactory;
import com.fnklabs.draenei.orm.DataProvider;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Analytics {
    private static final Logger LOGGER = LoggerFactory.getLogger(Analytics.class);
    private static final int V_NODES_SIZE = 256;
    private final MetricsFactory metricsFactory;
    @NotNull
    private final HazelcastInstance hazelcastInstance;
    @NotNull
    private final CassandraClient cassandraClient;

    public Analytics(@NotNull CassandraClient cassandraClient, @NotNull HazelcastInstance hazelcastInstance, MetricsFactory metricsFactory) {
        this.hazelcastInstance = hazelcastInstance;
        this.cassandraClient = cassandraClient;
        this.metricsFactory = metricsFactory;
    }


    /**
     * Load all data from DataProvider and execute ComputeTask over loaded data
     * <p>
     * Note: Reducer result will be grouped by key
     *
     * @param dataProvider      DataProvider from which will be loaded data
     * @param computeTask       Compute task
     * @param <KeyIn>           Key type
     * @param <ValueIn>         Value type
     * @param <KeyOut>          Output key from mapper
     * @param <ValueOut>        Output value from mapper
     * @param <ReducerValueOut> Output value from reducer
     *
     * @return Future for compute operation
     */
    public <KeyIn, ValueIn, KeyOut, ValueOut, ReducerValueOut> ListenableFuture<Map<KeyOut, ReducerValueOut>> compute(@NotNull DataProvider<ValueIn> dataProvider,
                                                                                                                      @NotNull ComputeTask<ValueIn, KeyOut, ValueOut, ReducerValueOut> computeTask) {
        UUID jobId = UUID.randomUUID();

        ListenableFuture<Integer> loadFuture = loadData(dataProvider, jobId);

        String mapName = LoadDataTask.getMapName(jobId);

        try {
            Integer loadedData = loadFuture.get(30, TimeUnit.MINUTES);

            LOGGER.info("Loaded data: {}", loadedData);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOGGER.warn("Can't execute operation", e);
        }

        JobTracker jobTracker = hazelcastInstance.getJobTracker("tv.nemo.content.dao.content_information");
        IMap<Long, ValueIn> map = hazelcastInstance.<Long, ValueIn>getMap(mapName);
        KeyValueSource<Long, ValueIn> source = KeyValueSource.fromMap(map);

        Job<Long, ValueIn> job = jobTracker.newJob(source);

        JobCompletableFuture<Map<KeyOut, ReducerValueOut>> submit = job.mapper(new HazelcastMapperWrapper<>(computeTask.getMapper()))
                                                                       .reducer(new HazelcastReducerFactory<KeyOut, ValueOut, ReducerValueOut>(computeTask.getReducer()))
                                                                       .submit();

        SettableFuture<Map<KeyOut, ReducerValueOut>> settableFuture = SettableFuture.<Map<KeyOut, ReducerValueOut>>create();

        submit.andThen(new ExecutionCallback<Map<KeyOut, ReducerValueOut>>() {
            @Override
            public void onResponse(Map<KeyOut, ReducerValueOut> response) {
                settableFuture.set(response);
                map.destroy();
            }

            @Override
            public void onFailure(Throwable t) {
                settableFuture.setException(t);
                map.destroy();
            }
        });

        return settableFuture;
    }

    @NotNull
    protected <ValueIn> ListenableFuture<Integer> loadData(@NotNull DataProvider<ValueIn> dataProvider, UUID jobId) {
        List<ListenableFuture<Integer>> futures = new ArrayList<>();
        BigInteger startToken = new BigInteger(String.valueOf(Long.MIN_VALUE));
        BigInteger maxValue = new BigInteger(String.valueOf(Long.MAX_VALUE));

        int membersSize = getCassandraClient().getMembers().size();

        int hazelcastNodes = getHazelcastInstance().getCluster().getMembers().size();

        BigInteger step = maxValue.divide(BigInteger.valueOf(hazelcastNodes * membersSize)).divide(BigInteger.valueOf(V_NODES_SIZE));

        while (startToken.compareTo(maxValue) <= 0) {

            BigInteger endToken = startToken.add(step);

            long startTokenLongValue = startToken.longValue();
            long endTokenLongValue = startTokenLongValue < endToken.longValue() ? endToken.longValue() : Long.MAX_VALUE;

            LoadDataTask<ValueIn> task = new LoadDataTask<>(startTokenLongValue, endTokenLongValue, jobId, dataProvider.getEntityClass());

            ICompletableFuture<Integer> submit = (ICompletableFuture<Integer>) hazelcastInstance.getExecutorService(getClass().getName()).submit(task);

            SettableFuture<Integer> responseFuture = SettableFuture.<Integer>create();
            futures.add(responseFuture);

            submit.andThen(new ExecutionCallback<Integer>() {
                @Override
                public void onResponse(Integer response) {
                    if (endTokenLongValue != Long.MAX_VALUE) {
                        LOGGER.info("Complete to load data: count {} in range [{},{})", response, startTokenLongValue, endTokenLongValue);
                    } else {
                        LOGGER.info("Complete to load data: count {} in range [{},{}]", response, startTokenLongValue, endTokenLongValue);
                    }

                    responseFuture.set(response);
                }

                @Override
                public void onFailure(Throwable t) {
                    LOGGER.warn("Can't complete operation", t);
                    responseFuture.set(null);
                }
            });

            startToken = endToken;
        }

        return Futures.transform(Futures.allAsList(futures), (List<Integer> futuresResult) -> {
            Optional<Integer> reduceResult = futuresResult.stream().reduce(Integer::sum);

            return reduceResult.isPresent() ? reduceResult.get() : 0;
        });
    }

    protected <K> DataProvider<K> getDataProvider(Class<K> entityClass) {
        return new DataProvider<>(entityClass, cassandraClient, hazelcastInstance, metricsFactory);
    }

    private IMap<UUID, JobStatus> getJobMap() {
        return hazelcastInstance.<UUID, JobStatus>getMap(StringUtils.lowerCase(getClass().getName()).concat(".job"));
    }

    @NotNull
    private HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    @NotNull
    private CassandraClient getCassandraClient() {
        return cassandraClient;
    }

    enum JobStatus {

    }


}
