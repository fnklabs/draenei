package com.fnklabs.draenei.analytics;

import com.google.common.base.Verify;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

abstract class TransformationFactory<InputKey, InputValue, OutputKey, OutputValue, CombinerOutput> extends ComputeTaskAdapter<TransformationContext<InputKey, InputValue, OutputKey, CombinerOutput>, Long> {
    @IgniteInstanceResource
    private Ignite ignite;

    @Nullable
    @Override
    public final Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            @Nullable TransformationContext<InputKey, InputValue, OutputKey, CombinerOutput> context
    ) throws IgniteException {
        Verify.verifyNotNull(context);

        return subgrid.stream()
                      .collect(Collectors.toMap(
                              clusterNode -> createTransformationFunction(context.getInputDataSource(), context.getOutputDataSource()),
                              clusterNode -> clusterNode,
                              (a, b) -> a
                      ));
    }

    @Nullable
    @Override
    public final Long reduce(List<ComputeJobResult> results) throws IgniteException {
        return results.stream()
                      .mapToLong(ComputeJobResult::<Long>getData)
                      .sum();
    }


    protected abstract TransformationFunction<InputKey, InputValue, OutputKey, OutputValue, CombinerOutput> createTransformationFunction(
            CacheConfiguration<InputKey, InputValue> inputData,
            CacheConfiguration<OutputKey, CombinerOutput> outputData
    );
}
