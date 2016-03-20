package com.fnklabs.draenei.orm;

import com.datastax.driver.core.HostDistance;
import com.fnklabs.draenei.CassandraClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.UUID;

@Ignore
public class UDTTest {
    private DataProvider<TestEntity> testDataProvider;

    @Before
    public void setUp() throws Exception {
        CassandraClient cassandraClient = new CassandraClient(null, null, "test", "10.211.55.19", HostDistance.LOCAL);

        testDataProvider = new DataProvider<TestEntity>(TestEntity.class, new CassandraClientFactory() {
            @Override
            public CassandraClient create() {

                return cassandraClient;
            }
        });

    }


    @Test
    public void testUdtType() throws Exception {
        TestUdt testUdt = new TestUdt("phone", "country", Arrays.asList("1", "2"));
        TestUdt testUdt1 = new TestUdt("phone1", "country2", Arrays.asList("1", "2"));
        TestUdt testUdt3 = new TestUdt("phone3", "country3", Arrays.asList("1", "2"));

        TestEntity testEntity = new TestEntity();
        testEntity.setId(UUID.randomUUID());
        testEntity.setTestUdt(testUdt);
        testEntity.setTestUdtList(Arrays.asList(testUdt, testUdt1));

        testDataProvider.save(testEntity);

        TestEntity one = testDataProvider.findOne(testEntity.getId());

        LoggerFactory.getLogger(getClass()).debug("Saved entity: {}", one);

        testEntity.setTestUdt(testUdt);
        testEntity.setTestUdtList(Arrays.asList(testUdt, testUdt1, testUdt3));

        testDataProvider.save(testEntity);

        one = testDataProvider.findOne(testEntity.getId());

        LoggerFactory.getLogger(getClass()).debug("Saved entity: {}", one);
    }

    @After
    public void tearDown() throws Exception {

    }
}
