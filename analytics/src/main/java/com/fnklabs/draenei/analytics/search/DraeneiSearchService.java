package com.fnklabs.draenei.analytics.search;

import com.fnklabs.draenei.analytics.TextUtils;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
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
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DraeneiSearchService implements Service, SearchService {


    private static final String CACHE_PREFIX = StringUtils.lowerCase(DraeneiSearchService.class.getName());

    /**
     * Documents cache name
     */
    private static final String CACHE_DOCUMENTS_NAME = CACHE_PREFIX + ".documents";

    private static final String CACHE_DOCUMENT_INDEX_NAME = CACHE_PREFIX + ".document_index";

    private String serviceName;
    /**
     * Ignite instance
     */
    @Nullable
    private transient Ignite ignite;
    /**
     * SimilarityFactory instance
     */
    @Nullable
    private transient SimilarityAlgorithm similarityAlgorithm;
    /**
     * ClusteringFactory instance
     */
    @Nullable
    private transient ClusteringAlgorithm clusteringAlgorithm;
    /**
     * Facet ranking algorithm
     */
    @Nullable
    private transient RankingAlgorithm rankingAlgorithm;
    /**
     * Cache for searchable documents
     */
    @Nullable
    private transient IgniteCache<Long, Document> documentsCache;
    @Nullable
    private transient IgniteCache<Long, DocumentIndex> documentIndexCache;
    private transient Predicate<Facet> NotStopWordPredicate;

    /**
     */
    public DraeneiSearchService() {
    }

    protected static CacheConfiguration<Long, DocumentIndex> getDocumentIndexCacheConfiguration() {
        CacheConfiguration<Long, DocumentIndex> cacheCfg = new CacheConfiguration<>(CACHE_DOCUMENT_INDEX_NAME);
        cacheCfg.setBackups(1);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setOffHeapMaxMemory(0);
        cacheCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        cacheCfg.setEvictionPolicy(new LruEvictionPolicy<>(10000));
        cacheCfg.setEvictSynchronized(false);

        return cacheCfg;
    }

    protected static CacheConfiguration<Long, Document> getDocumentsCacheConfiguration() {
        CacheConfiguration<Long, Document> cacheCfg = new CacheConfiguration<>(CACHE_DOCUMENTS_NAME);
        cacheCfg.setBackups(1);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setOffHeapMaxMemory(0);
        cacheCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        cacheCfg.setEvictionPolicy(new LruEvictionPolicy<>(10000));
        cacheCfg.setEvictSynchronized(false);


        return cacheCfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addDocument(Document document) {
        if (documentsCache != null && rankingAlgorithm != null && documentIndexCache != null) {
            Set<FacetRank> facetRanks = rankingAlgorithm.calculate(buildFacets(document));

            documentsCache.put(document.getId(), document);
            documentIndexCache.put(document.getId(), new DocumentIndex(document, facetRanks));
        }
    }

    @Override
    public <T extends Predicate<DocumentIndex> & Serializable> Collection<SearchResult> search(Collection<Facet> facets, T userPredicate) {
        Timer timer = MetricsFactory.getMetrics().getTimer("search_service.search");

        ClusterGroup clusterGroup = ignite.cluster().forServers();

        Collection<FacetRank> facetRanks = calculateRank(facets);
        List<SearchResult> execute = ignite.compute(clusterGroup)
                                           .execute(new SearchTask<T>(facetRanks, userPredicate), null);
        timer.stop();

        return execute;
    }

    @Override
    public Collection<SearchResult> getSimilar(long documentId) {
        Timer timer = MetricsFactory.getMetrics().getTimer("search_service.recommendation.get");

        ClusterGroup clusterGroup = ignite.cluster().forServers();

        DocumentIndex documentIndex = documentIndexCache.get(documentId);

        if (documentIndex != null) {
            List<SearchResult> execute = ignite.compute(clusterGroup)
                                               .execute(new SearchTask<>(documentIndex.getFacetRanks(), AlwaysTruePredicate.INSTANCE), null);

            timer.stop();
            return execute;
        } else {
            timer.stop();
            return Collections.emptyList();
        }
    }

    @Override
    public Collection<SearchResult> search(String text) {
        Timer timer = MetricsFactory.getMetrics().getTimer("search_service.search");

        Collection<SearchResult> searchResults = search(buildFacets(text), AlwaysTruePredicate.INSTANCE);

        timer.stop();

        return searchResults;
    }

    @Override

    public Collection<Facet> buildFacets(Document document) {
        return filter(clusteringAlgorithm.build(document));
    }

    @Override
    public Collection<Facet> buildFacets(String text) {
        return filter(clusteringAlgorithm.build(text));
    }

    /**
     * Calculate facets rank
     *
     * @param facets
     *
     * @return
     */
    @Override
    public Collection<FacetRank> calculateRank(Collection<Facet> facets) {
        return rankingAlgorithm.calculate(facets);
    }

    @Override
    public double calculateSimilarity(Collection<FacetRank> firstVector, Collection<FacetRank> secondVector) {
        return similarityAlgorithm.getSimilarity(firstVector, secondVector);
    }

    @Override
    public void cancel(ServiceContext ctx) {

    }

    @Override
    public void init(ServiceContext ctx) throws Exception {
        serviceName = ctx.name();

        if (ignite != null) {
            documentsCache = ignite.getOrCreateCache(getDocumentsCacheConfiguration());
            documentIndexCache = ignite.getOrCreateCache(getDocumentIndexCacheConfiguration());
        }

        setClusteringAlgorithm(createClusteringAlgorithm());
        setSimilarityAlgorithm(createSimilarityAlgorithm());
        setRankingAlgorithm(createRankingAlgorithm());
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {

    }

    protected void setNotStopWordPredicate(Predicate<Facet> notStopWordPredicate) {
        this.NotStopWordPredicate = notStopWordPredicate;
    }

    @Nullable
    protected Ignite getIgnite() {
        return ignite;
    }

    @IgniteInstanceResource
    protected void setIgnite(Ignite ignite) {
        this.ignite = ignite;
    }

    protected RankingAlgorithm createRankingAlgorithm() {
        return new RankingTfAlgorithm();
    }

    protected SimilarityAlgorithm createSimilarityAlgorithm() {
        return new SimilarityCosineAlgorithm();
    }

    protected ClusteringAlgorithm createClusteringAlgorithm() {
        return new ClusteringTermAlgorithm(new TextUtils());
    }

    protected void setSimilarityAlgorithm(@Nullable SimilarityAlgorithm similarityAlgorithm) {
        this.similarityAlgorithm = similarityAlgorithm;
    }

    protected void setClusteringAlgorithm(@Nullable ClusteringAlgorithm clusteringAlgorithm) {
        this.clusteringAlgorithm = clusteringAlgorithm;
    }

    protected void setRankingAlgorithm(@Nullable RankingAlgorithm rankingAlgorithm) {
        this.rankingAlgorithm = rankingAlgorithm;
    }

    /**
     * Filter stop word facets
     *
     * @param facets Input facets
     *
     * @return
     */
    private Collection<Facet> filter(Collection<Facet> facets) {
        if (NotStopWordPredicate == null) {
            return facets;
        }

        return facets.stream()
                     .filter(NotStopWordPredicate)
                     .collect(Collectors.toList());

    }
}
