package com.fnklabs.draenei.orm;

import com.datastax.driver.core.*;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.Metrics;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CacheableDataProviderTest {

    private HazelcastInstance hazelcastClient;
    private ListeningExecutorService executorService;
    private CacheableDataProvider<TestEntity> dataProvider;

    @Before
    public void setUp() throws Exception {
        hazelcastClient = Hazelcast.newHazelcastInstance();
        executorService = MoreExecutors.newDirectExecutorService();

        Metrics metricsFactory = new Metrics();

        dataProvider = new CacheableDataProvider<TestEntity>(TestEntity.class,
                new CassandraClient("", "", "test", "10.211.55.19", metricsFactory, MoreExecutors.newDirectExecutorService(), HostDistance.REMOTE),
                Hazelcast.newHazelcastInstance(),
                metricsFactory,
                MoreExecutors.newDirectExecutorService()
        );
    }

    @After
    public void tearDown() throws Exception {
        executorService.awaitTermination(15, TimeUnit.SECONDS);

        hazelcastClient.shutdown();
        executorService.shutdown();
        Metrics.report();
    }

    @Test
    public void testFindOneAsync() throws Exception {


    }

    @Test
    public void testSaveAsync() throws Exception {
        com.datastax.driver.core.ColumnMetadata columnMetadata = mock(com.datastax.driver.core.ColumnMetadata.class);
        when(columnMetadata.getType()).thenReturn(DataType.uuid());

        TableMetadata tableMetadataMock = mock(TableMetadata.class);

        List<com.datastax.driver.core.ColumnMetadata> columnMetadataList = Arrays.<com.datastax.driver.core.ColumnMetadata>asList(columnMetadata);

        when(tableMetadataMock.getPrimaryKey()).thenReturn(columnMetadataList);
        when(tableMetadataMock.getColumn(anyString())).thenReturn(columnMetadata);

        ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
        when(columnDefinitions.size()).thenReturn(1);

        PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
        when(preparedStatementMock.getVariables()).thenReturn(columnDefinitions);
        when(preparedStatementMock.getPreparedId()).thenReturn(mock(PreparedId.class));

        CassandraClient cassandraClientMock = mock(CassandraClient.class);
        when(cassandraClientMock.getTableMetadata(anyString())).thenReturn(tableMetadataMock);
        when(cassandraClientMock.prepare(anyString())).thenReturn(preparedStatementMock);

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.wasApplied()).thenReturn(true);

        ResultSetFuture resultSetFuture = mock(ResultSetFuture.class);
        when(resultSetFuture.isDone()).thenReturn(true);
        when(resultSetFuture.isCancelled()).thenReturn(false);
        when(resultSetFuture.get()).thenReturn(resultSet);
        when(resultSetFuture.getUninterruptibly()).thenReturn(resultSet);
        when(resultSetFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(resultSet);


        when(cassandraClientMock.executeAsync(any(BoundStatement.class))).thenReturn(resultSetFuture);

        CacheableDataProvider<TestEntity> testEntityCacheableDataProvider = new CacheableDataProvider<TestEntity>(TestEntity.class,
                cassandraClientMock,
                hazelcastClient,
                new Metrics(),
                executorService
        );

        for (int i = 0; i < 1000; i++) {
            testEntityCacheableDataProvider.saveAsync(new TestEntity());
        }
    }

    @Test
    public void testSave() throws Exception {
        for (int i = 0; i < 100; i++) {
            Boolean result = dataProvider.saveAsync(new com.fnklabs.draenei.orm.TestEntity()).get(5, TimeUnit.SECONDS);

            Assert.assertTrue(result);
        }

        Metrics.report();
    }

    @Test
    public void testSave2() throws Exception {
        for (int i = 0; i < 100; i++) {
            Boolean result = dataProvider.saveAsync(new com.fnklabs.draenei.orm.TestEntity()).get(5, TimeUnit.SECONDS);

            Assert.assertTrue(result);
        }

        Metrics.report();
    }

    @Test
    public void testRemoveAsync() throws Exception {

    }


}