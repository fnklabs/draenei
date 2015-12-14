package com.fnklabs.draenei.orm.analytics;

import com.fnklabs.draenei.CassandraClient;
import com.google.common.collect.Range;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collection;

public class CassandraUtilsTest {

    @Test
    public void testSplitRing() throws Exception {
        CassandraClient cassandraClient = Mockito.mock(CassandraClient.class);
        Collection<Range<Long>> ranges = CassandraUtils.splitRing(cassandraClient);

        Assert.assertNotNull(ranges);

        Assert.assertFalse(ranges.isEmpty());
    }
}