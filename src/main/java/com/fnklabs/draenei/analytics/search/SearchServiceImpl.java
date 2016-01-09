package com.fnklabs.draenei.analytics.search;

import com.codahale.metrics.Timer;
import com.fnklabs.draenei.MetricsFactory;
import com.fnklabs.draenei.MetricsFactoryImpl;
import com.fnklabs.draenei.analytics.TextUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SearchServiceImpl implements Service, SearchService {
    public static final String SERVICE_NAME = "search_service";

    private static final String CACHE_PREFIX = StringUtils.lowerCase(SearchServiceImpl.class.getName());

    /**
     * Documents cache name
     */
    private static final String CACHE_DOCUMENTS_NAME = CACHE_PREFIX + ".documents";

    private static final String CACHE_DOCUMENT_INDEX_NAME = CACHE_PREFIX + ".document_index";
    /**
     * Facets cache name
     */
    private static final String CACHE_FACETS_NAME = CACHE_PREFIX + ".facets";

    private static final Class<? extends ClusteringAlgorithm> DEFAULT_CLUSTERING_ALGORITHM = ClusteringTfAlgorithm.class;

    private String serviceName;

    /**
     * Ignite instance
     */
    @NotNull
    private transient Ignite ignite;
    /**
     * SimilarityFactory instance
     */
    @NotNull
    private transient SimilarityAlgorithmFactory similarityAlgorithmFactory;
    /**
     * ClusteringFactory instance
     */
    @NotNull
    private ClusteringAlgorithmFactory clusteringAlgorithmFactory = new ClusteringAlgorithmFactory();
    /**
     * Cache for searchable documents
     */
    private transient IgniteCache<Long, Document> documentsCache;

    private transient MetricsFactory metricsFactory;


    /**
     */
    public SearchServiceImpl() {
    }

    @Override
    public void addDocument(@NotNull Document document) {
        documentsCache.put(document.getId(), document);

        Set<Facet> facets = buildFacets(document);


        IgniteCache<Long, DocumentIndex> cache = ignite.getOrCreateCache(getDocumentIndexCacheConfiguration());

        cache.put(document.getId(), new DocumentIndex(document, facets));

//        facets.forEach(facet -> {
//            facetsCache.invoke(facet.getKey(), new UpdateFacetsIndex(Sets.newHashSet(facet)));
//        });

    }


    @NotNull
    @Override
    public List<SearchResult> search(@NotNull String text) {
        Timer.Context timer = MetricsFactoryImpl.getTimer("search_service.search").time();

        Set<Facet> facets = buildFacets(text);

        List<SearchResult> execute = search(facets);

        timer.stop();

        return execute;
    }

    @NotNull
    @Override
    public List<SearchResult> search(@NotNull Set<Facet> facets) {
        Timer.Context timer = MetricsFactoryImpl.getTimer("search_service.search").time();

        ClusterGroup clusterGroup = ignite.cluster().forServers();

        List<SearchResult> execute = ignite.compute(clusterGroup)
                                           .execute(new SearchTask(facets), null);

        timer.stop();

        return execute;
    }


    @NotNull
    @Override
    public List<SearchResult> getRecommendation(long documentId) {
        Timer.Context timer = MetricsFactoryImpl.getTimer("search_service.recommendation.get").time();

        ClusterGroup clusterGroup = ignite.cluster().forServers();

        IgniteCache<Long, DocumentIndex> cache = ignite.getOrCreateCache(SearchServiceImpl.getDocumentIndexCacheConfiguration());
        DocumentIndex documentIndex = cache.get(documentId);

        if (documentIndex != null) {
            List<SearchResult> execute = ignite.compute(clusterGroup)
                                               .execute(new SearchTask(documentIndex.getFacets()), null);

            timer.stop();
            return execute;
        } else {
            timer.stop();
            return Collections.emptyList();
        }


    }

    @Override
    public void rebuildDocumentIndex() {
        ClusterGroup clusterGroup = ignite.cluster().forServers();

        RebuildTfIndexTask rebuildTfIndexTask = new RebuildTfIndexTask(getDocumentsCacheConfiguration(), getFacetsCacheConfiguration());

        int result = ignite.compute(clusterGroup)
                           .execute(rebuildTfIndexTask, null);

        LoggerFactory.getLogger(getClass()).debug("Indexed documents: {}", result);
    }

    @Override
    public void cancel(ServiceContext ctx) {

    }

    @Override
    public void init(ServiceContext ctx) throws Exception {
        serviceName = ctx.name();

        documentsCache = ignite.getOrCreateCache(getDocumentsCacheConfiguration());

        metricsFactory = new MetricsFactoryImpl();

        clusteringAlgorithmFactory.registerClusteringAlgorithm(new ClusteringTfAlgorithm(new TextUtils(metricsFactory)));
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {

    }

    @Override
    @NotNull
    public Set<Facet> buildFacets(@NotNull Document document) {
        return getClusteringAlgorithm().build(document.getDocument());
    }

    @NotNull
    @Override
    public Set<Facet> buildFacets(@NotNull String text) {
        return getClusteringAlgorithm().build(text);
    }

    @IgniteInstanceResource
    protected void setIgnite(@NotNull Ignite ignite) {
        this.ignite = ignite;
    }

    @NotNull
    private ClusteringAlgorithm getClusteringAlgorithm() {
        return clusteringAlgorithmFactory.get(DEFAULT_CLUSTERING_ALGORITHM);
    }

    @NotNull
    protected static CacheConfiguration<Long, DocumentIndex> getDocumentIndexCacheConfiguration() {
        CacheConfiguration<Long, DocumentIndex> cacheCfg = new CacheConfiguration<>(CACHE_DOCUMENT_INDEX_NAME);
        cacheCfg.setBackups(1);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setOffHeapMaxMemory(0);
        cacheCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        cacheCfg.setEvictionPolicy(new LruEvictionPolicy<>(100000));
        cacheCfg.setEvictSynchronized(false);

        return cacheCfg;
    }

    @NotNull
    protected static CacheConfiguration<FacetKey, Set<Facet>> getFacetsCacheConfiguration() {
        CacheConfiguration<FacetKey, Set<Facet>> cacheCfg = new CacheConfiguration<>(CACHE_FACETS_NAME);
        cacheCfg.setBackups(1);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setOffHeapMaxMemory(0);
        cacheCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        cacheCfg.setEvictionPolicy(new LruEvictionPolicy<>(10000));
        cacheCfg.setEvictSynchronized(false);

        return cacheCfg;
    }

    @NotNull
    protected static CacheConfiguration<Long, Document> getDocumentsCacheConfiguration() {
        CacheConfiguration<Long, Document> cacheCfg = new CacheConfiguration<>(CACHE_DOCUMENTS_NAME);
        cacheCfg.setBackups(1);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setOffHeapMaxMemory(0);
        cacheCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        cacheCfg.setEvictionPolicy(new LruEvictionPolicy<>(100000));
        cacheCfg.setEvictSynchronized(false);


        return cacheCfg;
    }
}
