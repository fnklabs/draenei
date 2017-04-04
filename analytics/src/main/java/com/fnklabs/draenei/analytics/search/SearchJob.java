package com.fnklabs.draenei.analytics.search;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;

import javax.cache.Cache;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;


public class SearchJob<T extends Predicate<DocumentIndex> & Serializable> extends ComputeJobAdapter {

    private final Collection<FacetRank> searchFacetRanks;
    private final T userPredicate;
    private Ignite ignite;

    public SearchJob(Collection<FacetRank> searchFacetRanks, T userPredicate) {
        this.searchFacetRanks = searchFacetRanks;
        this.userPredicate = userPredicate;
    }

    @Override
    public Set<DocumentIndex> execute() throws IgniteException {
        IgniteCache<Long, DocumentIndex> cache = ignite.getOrCreateCache(DraeneiSearchService.getDocumentIndexCacheConfiguration());

        Iterable<Cache.Entry<Long, DocumentIndex>> localEntries = cache.localEntries(CachePeekMode.PRIMARY);

        Set<DocumentIndex> result = new HashSet<>();

        localEntries.forEach(localEntry -> {
            if (userPredicate.test(localEntry.getValue())) {
                DocumentIndex documentIndex = localEntry.getValue();

                searchFacetRanks.forEach(searchFacet -> {

                    boolean anyMatch = documentIndex.getFacetRanks()
                                                    .stream()
                                                    .anyMatch(facet -> Facet.same(searchFacet.getKey(), facet.getKey()));


                    if (anyMatch) {
                        result.add(documentIndex);
                    }
                });
            }
        });

        return result;
    }

    @IgniteInstanceResource
    protected void setIgnite(Ignite ignite) {
        this.ignite = ignite;
    }


}
