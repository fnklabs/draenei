package com.fnklabs.draenei.analytics.search;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.ServiceResource;
import org.jetbrains.annotations.NotNull;

import javax.cache.Cache;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;


/**
 * Job for calculating TF facets from Documents cache (local)
 */
class RebuildTfIndexJob extends ComputeJobAdapter {

    /**
     * Cache configuration
     */
    private final CacheConfiguration<Long, Document> documentCacheConfiguration;

    private final CacheConfiguration<FacetKey, Set<Facet>> facetsCacheConfiguration;

    /**
     * Ignite instance
     */
    private Ignite ignite;


    private SearchServiceImpl searchService;

    RebuildTfIndexJob(@NotNull CacheConfiguration<Long, Document> documentCacheConfiguration, CacheConfiguration<FacetKey, Set<Facet>> facetsCacheConfiguration) {
        this.documentCacheConfiguration = documentCacheConfiguration;
        this.facetsCacheConfiguration = facetsCacheConfiguration;
    }

    @Override
    public Integer execute() throws IgniteException {
        IgniteCache<Long, Document> documentIgniteCache = ignite.getOrCreateCache(documentCacheConfiguration);

        Iterable<Cache.Entry<Long, Document>> entriesIterable = documentIgniteCache.localEntries(CachePeekMode.PRIMARY);

        AtomicInteger count = new AtomicInteger(0);

        Map<FacetKey, Set<Facet>> facetsMap = new HashMap<>();

        entriesIterable.forEach(entry -> {
            count.getAndIncrement();

            Set<Facet> facets = searchService.buildFacets(entry.getValue());

            facets.forEach(facet -> {
                facetsMap.compute(facet.getKey(), new BiFunction<FacetKey, Set<Facet>, Set<Facet>>() {
                    @Override
                    public Set<Facet> apply(FacetKey facetKey, Set<Facet> facets) {

                        if (facets == null) {
                            facets = new HashSet<Facet>();
                        }

                        facets.add(facet);

                        return facets;
                    }
                });
            });
        });

        IgniteCache<FacetKey, Set<Facet>> cache = ignite.getOrCreateCache(facetsCacheConfiguration);

        facetsMap.forEach((key, values) -> {
            cache.invoke(key, new UpdateFacetsIndex(values));
        });

        return count.intValue();
    }

    @ServiceResource(serviceName = SearchServiceImpl.SERVICE_NAME)
    protected void setSearchService(SearchServiceImpl searchService) {
        this.searchService = searchService;
    }

    @IgniteInstanceResource
    protected void setIgnite(Ignite ignite) {
        this.ignite = ignite;
    }
}
