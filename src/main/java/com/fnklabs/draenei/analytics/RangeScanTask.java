package com.fnklabs.draenei.analytics;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.TokenRange;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.orm.DataProvider;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class RangeScanTask<Entity> extends ComputeTaskAdapter<Object, Integer> {

    @IgniteInstanceResource
    private Ignite ignite;

    @Nullable
    @Override
    public final Map<? extends RangeScanMapper, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) throws IgniteException {

        Map<RangeScanMapper, ClusterNode> tasks = new HashMap<>();

        Map<TokenRange, Set<Host>> tokenRangeOwners = getCassandraClient().getTokenRangeOwners(getDataProvider().getKeyspace());

        tokenRangeOwners.forEach((tokenRange, hosts) -> {
            Host host = hosts.stream().findAny().orElse(null);

            String hostAddress = host.getAddress().getHostAddress();

            Optional<ClusterNode> nearNode = subgrid.stream()
                                                    .filter(node -> node.addresses().contains(hostAddress))
                                                    .findFirst();


            ClusterNode clusterNode = nearNode.orElse(subgrid.stream().findAny().orElse(null));

            LoggerFactory.getLogger(getClass()).debug("Nearest node for cassandra host {} is {}", host.getAddress().getHostAddress(), clusterNode.addresses());

            tasks.put(createMapper(tokenRange), clusterNode);
        });

        return tasks;
    }

    @Nullable
    @Override
    public final Integer reduce(List<ComputeJobResult> results) throws IgniteException {
        List<Integer> nodesResponse = new ArrayList<>();

        for (ComputeJobResult res : results) {
            Integer data = res.<Integer>getData();
            nodesResponse.add(data);
        }

        return nodesResponse.stream().mapToInt(value -> value).sum();
    }


    protected abstract RangeScanMapper createMapper(TokenRange tokenRange);

    protected abstract DataProvider<Entity> getDataProvider();

    protected Ignite getIgnite() {
        return ignite;
    }

    abstract protected CassandraClient getCassandraClient();
}
