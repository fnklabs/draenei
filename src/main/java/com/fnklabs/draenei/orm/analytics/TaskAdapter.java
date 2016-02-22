package com.fnklabs.draenei.orm.analytics;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TaskAdapter<KeyIn, ValueIn, ArgumentValue, ValueOut, ReduceResult> extends ComputeTaskAdapter<ArgumentValue, ReduceResult> {
    @IgniteInstanceResource
    private Ignite ignite;

    private final MapFunction<KeyIn, ValueIn, ValueOut> mapFunction;

    private final ReduceFunction<ValueOut, ReduceResult> reduceFunction;

    private final CacheConfiguration<KeyIn, ValueIn> cacheConfiguration;

    TaskAdapter(MapFunction<KeyIn, ValueIn, ValueOut> mapFunction, ReduceFunction<ValueOut, ReduceResult> reduceFunction, CacheConfiguration<KeyIn, ValueIn> cacheConfiguration) {
        this.mapFunction = mapFunction;
        this.reduceFunction = reduceFunction;
        this.cacheConfiguration = cacheConfiguration;
    }

    @Nullable
    @Override
    public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable ArgumentValue arg) throws IgniteException {
        Map<ComputeJob, ClusterNode> map = new HashMap<>();


        subgrid.forEach(node -> {
            map.put(new ReadLocalDataFromCache<KeyIn, ValueIn, ValueOut>(cacheConfiguration, mapFunction), node);
        });

        return map;
    }

    @Nullable
    @Override
    public ReduceResult reduce(List<ComputeJobResult> results) throws IgniteException {
        List<ValueOut> nodesResponse = new ArrayList<>();

        for (ComputeJobResult res : results) {
            List<ValueOut> data = res.<List<ValueOut>>getData();
            nodesResponse.addAll(data);
        }

        if (reduceFunction instanceof IgniteInstanceAware) {
            ((IgniteInstanceAware) reduceFunction).setIgnite(ignite);
        }

        ReduceResult reduce = reduceFunction.reduce(nodesResponse);

        return reduce;
    }
}
