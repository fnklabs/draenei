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

public class MapReduceTask<KeyIn, ValueIn, ValueOut, ReduceResult> extends ComputeTaskAdapter<Object, ReduceResult> {
    private final CacheConfiguration<KeyIn, ValueIn> cacheConfiguration;
    @IgniteInstanceResource
    private Ignite ignite;

    protected MapReduceTask(CacheConfiguration<KeyIn, ValueIn> cacheConfiguration) {
        this.cacheConfiguration = cacheConfiguration;
    }

    @Nullable
    @Override
    public final Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) throws IgniteException {
        Map<ComputeJob, ClusterNode> map = new HashMap<>();


        subgrid.forEach(node -> {
            CacheMapper<KeyIn, ValueIn, ValueOut> cacheMapper = getCacheMapper();

            map.put(cacheMapper, node);
        });

        return map;
    }

    @Nullable
    @Override
    public final ReduceResult reduce(List<ComputeJobResult> results) throws IgniteException {
        List<ValueOut> nodesResponse = new ArrayList<>();

        for (ComputeJobResult res : results) {
            List<ValueOut> data = res.<List<ValueOut>>getData();
            nodesResponse.addAll(data);
        }

        return finalize(nodesResponse);
    }

    protected CacheConfiguration<KeyIn, ValueIn> getCacheConfiguration() {
        return cacheConfiguration;
    }

    protected ReduceResult finalize(List<ValueOut> result) {
        return null;
    }


    @NotNull
    protected CacheMapper<KeyIn, ValueIn, ValueOut> getCacheMapper() {
        return new LogMapper<KeyIn, ValueIn, ValueOut>(cacheConfiguration);
    }

    private static class LogMapper<KeyIn, ValueIn, ValueOut> extends CacheMapper<KeyIn, ValueIn, ValueOut> {

        LogMapper(CacheConfiguration<KeyIn, ValueIn> cacheConfiguration) {
            super(cacheConfiguration);
        }

        @Override
        protected ValueOut map(KeyIn keyIn, ValueIn valueIn) {
            LoggerFactory.getLogger(LogMapper.class).debug("Input data {}:{}", keyIn, valueIn);
            return null;
        }
    }
}
