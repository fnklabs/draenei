package com.fnklabs.draenei.analytics;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.TokenRange;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.orm.DataProvider;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.*;

/**
 * Task for scanning dataprovider by token range
 *
 * @param <Entity>
 */
public abstract class RangeScanJobFactory<Entity, Key, Value> extends ComputeTaskAdapter<Object, Integer> {

    private static final MathContext MATH_CONTEXT = new MathContext(2, RoundingMode.HALF_EVEN);

    private CacheConfiguration<Key, Value> cacheConfiguration;

    @IgniteInstanceResource
    private transient Ignite ignite;

    private int totalTasks;

    public void setCacheConfiguration(CacheConfiguration<Key, Value> cacheConfiguration) {
        this.cacheConfiguration = cacheConfiguration;
    }

    @Nullable
    @Override
    public final Map<? extends RangeScanJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) throws IgniteException {
        Map<RangeScanJob, ClusterNode> tasks = new HashMap<>();

        Map<Host, Set<TokenRange>> rangeScanTask = AnalyticsUtils.splitRangeScanTask(getDataProvider().getKeyspace(), getCassandraClient());

        rangeScanTask.forEach((host, tokenRanges) -> {
            String hostAddress = host.getAddress().getHostAddress();

            Optional<ClusterNode> nearNode = subgrid.stream()
                                                    .filter(node -> node.addresses().contains(hostAddress))
                                                    .findFirst();

            ClusterNode clusterNode = nearNode.orElse(subgrid.stream().findAny().orElse(null));

            getLogger().debug(
                    "Nearest node for cassandra host {} is {}",
                    host.getAddress().getHostAddress(),
                    clusterNode.addresses()
            );

            tokenRanges.stream()
                       .forEach(tokenRange -> tasks.put(createJob(tokenRange, cacheConfiguration), clusterNode));
        });

        totalTasks = tasks.size();

        return tasks;
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

    protected abstract RangeScanJob createJob(TokenRange tokenRange, CacheConfiguration<Key, Value> cacheConfiguration);

    protected abstract DataProvider<Entity> getDataProvider();

    protected abstract CassandraClient getCassandraClient();
}
