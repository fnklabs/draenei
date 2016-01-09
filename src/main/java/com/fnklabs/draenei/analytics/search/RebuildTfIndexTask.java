package com.fnklabs.draenei.analytics.search;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class RebuildTfIndexTask extends ComputeTaskAdapter<Document, Integer> {

    @NotNull
    private final CacheConfiguration<Long, Document> documentsCacheConfiguration;

    @NotNull
    private final CacheConfiguration<FacetKey, Set<Facet>> facetCacheConfiguration;

    private Ignite ignite;

    RebuildTfIndexTask(@NotNull CacheConfiguration<Long, Document> documentsCacheConfiguration, @NotNull CacheConfiguration<FacetKey, Set<Facet>> facetCacheConfiguration) {
        this.documentsCacheConfiguration = documentsCacheConfiguration;
        this.facetCacheConfiguration = facetCacheConfiguration;
    }

    @Nullable
    @Override
    public Map<RebuildTfIndexJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Document arg) throws IgniteException {
        HashMap<RebuildTfIndexJob, ClusterNode> calculateTfJobClusterNodeHashMap = new HashMap<>();

        subgrid.forEach(clusterNode -> {
            calculateTfJobClusterNodeHashMap.put(new RebuildTfIndexJob(documentsCacheConfiguration, facetCacheConfiguration), clusterNode);
        });

        return calculateTfJobClusterNodeHashMap;
    }

    @Nullable
    @Override
    public Integer reduce(List<ComputeJobResult> results) throws IgniteException {
        int sum = results.stream()
                         .mapToInt(computeJobResult -> {
                             return computeJobResult.<Integer>getData();
                         })
                         .sum();

        return sum;
    }

    @IgniteInstanceResource
    protected void setIgnite(Ignite ignite) {
        this.ignite = ignite;
    }
}
