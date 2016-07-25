package com.fnklabs.draenei.analytics;

import com.datastax.driver.core.TokenRange;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.orm.DataProvider;
import com.google.common.net.HostAndPort;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Task for scanning dataprovider by token range
 *
 * @param <Entity>
 */
public abstract class RangeScanJobFactory<Entity, Key, Value, CombinerOutputValue> extends ComputeTaskAdapter<CacheConfiguration<Key, CombinerOutputValue>, Integer> {

    private static final MathContext MATH_CONTEXT = new MathContext(2, RoundingMode.HALF_EVEN);


    @IgniteInstanceResource
    private transient Ignite ignite;

    private int totalTasks;

    @Nullable
    @Override
    public final Map<? extends RangeScanJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable CacheConfiguration<Key, CombinerOutputValue> cacheConfiguration) throws IgniteException {
        Map<TokenRange, ClusterNode> rangeScanTask = AnalyticsContext.splitRangeScanTask(getDataProvider().getKeyspace(), getCassandraClient(), subgrid);

        Map<RangeScanJob, ClusterNode> jobs = rangeScanTask.entrySet()
                                                           .stream()
                                                           .collect(
                                                                   Collectors.toMap(
                                                                           entry -> createJob(entry.getKey(), cacheConfiguration),
                                                                           Map.Entry::getValue
                                                                   )
                                                           );


        totalTasks = jobs.size();

        return jobs;
    }

    @Override
    public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        ComputeJobResultPolicy result = super.result(res, rcvd);

        if (result == ComputeJobResultPolicy.WAIT) {
            float completedTasks = (float) (rcvd.size()) / totalTasks * 100;
            getLogger().debug(
                    "Completed: {}/{} ({}%)",
                    rcvd.size(),
                    totalTasks,
                    BigDecimal.valueOf(completedTasks).round(MATH_CONTEXT)
            );
        }

        return result;
    }

    @Nullable
    @Override
    public final Integer reduce(List<ComputeJobResult> results) throws IgniteException {
        List<Integer> nodesResponse = new ArrayList<>();

        for (ComputeJobResult res : results) {
            Integer data = res.<Integer>getData();
            nodesResponse.add(data);
        }

        return nodesResponse.stream()
                            .mapToInt(value -> value)
                            .sum();
    }

    protected Logger getLogger() {
        return LoggerFactory.getLogger(getClass());
    }

    protected Ignite getIgnite() {
        return ignite;
    }

    protected abstract RangeScanJob<Entity, Key, Value, CombinerOutputValue> createJob(TokenRange tokenRange, CacheConfiguration<Key, CombinerOutputValue> cacheConfiguration);

    protected abstract DataProvider<Entity> getDataProvider();

    protected abstract CassandraClient getCassandraClient();
}
