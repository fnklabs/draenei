package com.fnklabs.draenei.analytics.search;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;

import javax.cache.Cache;
import java.util.HashSet;
import java.util.Set;


class SearchJob extends ComputeJobAdapter {

    private Ignite ignite;

    private final Set<Facet> searchFacets;

    SearchJob(Set<Facet> searchFacets) {
        this.searchFacets = searchFacets;
    }

    @Override
    public Set<DocumentIndex> execute() throws IgniteException {
        IgniteCache<Long, DocumentIndex> cache = ignite.getOrCreateCache(SearchServiceImpl.getDocumentIndexCacheConfiguration());


        Iterable<Cache.Entry<Long, DocumentIndex>> localEntries = cache.localEntries(CachePeekMode.PRIMARY);

        Set<DocumentIndex> result = new HashSet<>();

        localEntries.forEach(localEntry -> {

            searchFacets.forEach(searchFacet -> {
                DocumentIndex documentIndex = localEntry.getValue();

                boolean anyMatch = documentIndex.getFacets()
                                                .stream()
                                                .anyMatch(facet -> facet.getKey().equals(searchFacet.getKey()));


                if (anyMatch) {
                    result.add(documentIndex);
                }
            });

        });

        return result;
    }

    @IgniteInstanceResource
    protected void setIgnite(Ignite ignite) {
        this.ignite = ignite;
    }


}
