package com.fnklabs.draenei.orm.analytics;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.Metrics;
import com.fnklabs.draenei.MetricsFactory;
import com.fnklabs.draenei.orm.CassandraClientFactory;
import com.fnklabs.draenei.orm.DataProvider;
import com.fnklabs.draenei.orm.TestEntity;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnalyticsTest {

    @Ignore
    @Test
    public void testCompute() throws Exception {
        Metrics metricsFactory = new Metrics();
        CassandraClient cassandraClient = new CassandraClient("", "", "test", "10.211.55.19", metricsFactory, HostDistance.LOCAL);

        AnalyticsManagedContext managedContext = new AnalyticsManagedContext();

        Config config = new Config();
        config.setManagedContext(managedContext);

        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        Analytics analytics = new Analytics(
                cassandraClient,
                hazelcastInstance,
                metricsFactory
        );

        managedContext.setAnalyticsContext(analytics);

        DataProvider<TestEntity> testEntityDataProvider = new DataProvider<>(TestEntity.class, mock(CassandraClientFactory.class), metricsFactory);

        ListenableFuture<Map<Boolean, Integer>> computeFuture = analytics.compute(testEntityDataProvider, new ComputeTask<TestEntity, Boolean, Integer, Integer>() {
            @NotNull
            @Override
            public Mapper<TestEntity, Boolean, Integer> getMapper() {
                return new Mapper<TestEntity, Boolean, Integer>() {
                    @Override
                    public void map(long keyIn, TestEntity testEntity, Context<Boolean, Integer> context) {
                        context.emit(true, 1);
                    }
                };
            }

            @Nullable
            @Override
            public Reducer<Boolean, Integer, Integer> getReducer() {
                return new Reducer<Boolean, Integer, Integer>() {
                    @Override
                    public Integer reduce(Boolean aBoolean, Iterable<Integer> integers) {
                        int sum = 0;

                        for (Integer integer : integers) {
                            sum += integer;
                        }
                        return sum;
                    }
                };
            }
        });

        Map<Boolean, Integer> result = computeFuture.get(15, TimeUnit.SECONDS);

        Assert.assertNotNull(result);

        LoggerFactory.getLogger(getClass()).debug("Result: {}", result);
    }

    @Test
    public void testLoadData() throws Exception {
        CassandraClient cassandraClient = mock(CassandraClient.class);
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);

        Analytics analytics = new Analytics(
                cassandraClient,
                hazelcastInstance,
                mock(MetricsFactory.class)
        );

        Cluster cluster = mock(Cluster.class);

        when(cassandraClient.getMembers()).thenReturn(Sets.newHashSet(mock(Host.class), mock(Host.class)));
        when(hazelcastInstance.getCluster()).thenReturn(cluster);
        when(cluster.getMembers()).thenReturn(Sets.newHashSet(mock(Member.class), mock(Member.class)));


        IExecutorService executorService = mock(IExecutorService.class);
        when(executorService.submit(any(Callable.class))).thenReturn(new ICompletableWrapper());

        when(hazelcastInstance.getExecutorService(anyString())).thenReturn(executorService);

        ListenableFuture<Integer> loadFuture = analytics.loadData(mock(DataProvider.class), UUID.randomUUID().toString());

        BigInteger startToken = new BigInteger(String.valueOf(Long.MIN_VALUE));
        BigInteger maxValue = new BigInteger(String.valueOf(Long.MAX_VALUE));

        int membersSize = cassandraClient.getMembers().size();
        int hazelcastNodes = hazelcastInstance.getCluster().getMembers().size();

        BigInteger step = maxValue.divide(BigInteger.valueOf(membersSize * hazelcastNodes)).divide(BigInteger.valueOf(256));

        int count = 0;
        while (startToken.compareTo(maxValue) <= 0) {
            startToken = startToken.add(step);
            count++;
        }
        Integer result = loadFuture.get(5, TimeUnit.SECONDS);

        Assert.assertNotNull(result);
        Assert.assertEquals(count, result.intValue());
    }

    private static class ICompletableWrapper implements ICompletableFuture<Object> {

        @Override
        public void andThen(ExecutionCallback<Object> callback) {
            callback.onResponse(1);
        }

        @Override
        public void andThen(ExecutionCallback<Object> callback, Executor executor) {

        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public Integer get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public Integer get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
    }
}