package com.fnklabs.draenei.analytics;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class MapReduceTaskFactory<KeyIn, ValueIn, ValueOut, ReduceResult> extends ComputeTaskAdapter<Object, ReduceResult> {
    private CacheConfiguration<KeyIn, ValueIn> cacheConfiguration;

    @IgniteInstanceResource
    private Ignite ignite;

    @Nullable
    @Override
    public final Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) throws IgniteException {
        return subgrid.stream()
                      .collect(Collectors.toMap(
                              clusterNode -> createCacheMapper(cacheConfiguration),
                              clusterNode -> clusterNode,
                              (a, b) -> a
                      ));
    }

    @Nullable
    @Override
    public final ReduceResult reduce(List<ComputeJobResult> results) throws IgniteException {
        List<ValueOut> nodesResponse = new ArrayList<>();

        for (ComputeJobResult res : results) {
            ValueOut data = res.getData();
            nodesResponse.add(data);
        }

        return finalize(nodesResponse);
    }

    public void setCacheConfiguration(CacheConfiguration<KeyIn, ValueIn> cacheConfiguration) {
        this.cacheConfiguration = cacheConfiguration;
    }

    protected abstract ReduceResult finalize(List<ValueOut> result);

    @NotNull
    protected abstract MapperTask<KeyIn, ValueIn, ValueOut> createCacheMapper(CacheConfiguration<KeyIn, ValueIn> cacheConfiguration);
}
