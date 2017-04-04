package com.fnklabs.draenei.analytics;

import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import org.apache.ignite.cluster.ClusterNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AnalyticsContextTest {
    private List<ClusterNode> clusterNodes;

    @Mock
    private ClusterNode clusterNode1;
    @Mock
    private ClusterNode clusterNode2;

    private HostAndPort firstDataNode;

    private HostAndPort secondDataNode;

    @Before
    public void setUp() throws Exception {
        clusterNodes = Arrays.asList(clusterNode1, clusterNode2);

        when(clusterNode1.addresses()).thenReturn(Arrays.asList("127.0.0.1"));
        when(clusterNode2.addresses()).thenReturn(Arrays.asList("127.0.0.2"));

        firstDataNode = HostAndPort.fromParts("127.0.0.1", 9000);
        secondDataNode = HostAndPort.fromParts("127.0.0.2", 9000);
    }

    @Test
    public void getLessLoadedNode() throws Exception {
        HashMap<HostAndPort, Integer> dataOwnerStatistic = new HashMap<>();
        dataOwnerStatistic.put(firstDataNode, 1);

        ClusterNode lessLoadedNode = AnalyticsContext.getLessLoadedNode(clusterNodes, dataOwnerStatistic, Sets.newHashSet(firstDataNode, secondDataNode));

        Assert.assertEquals(lessLoadedNode, clusterNode2);
    }


    @Test
    public void getLessLoadedNode2() throws Exception {
        HashMap<HostAndPort, Integer> dataOwnerStatistic = new HashMap<>();
        dataOwnerStatistic.put(firstDataNode, 2);
        dataOwnerStatistic.put(secondDataNode, 1);

        ClusterNode lessLoadedNode = AnalyticsContext.getLessLoadedNode(clusterNodes, dataOwnerStatistic, Sets.newHashSet(firstDataNode, secondDataNode));

        Assert.assertEquals(lessLoadedNode, clusterNode2);
    }
}