package com.fnklabs.draenei.analytics.search;

import com.codahale.metrics.Timer;
import com.fnklabs.draenei.MetricsFactoryImpl;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.jetbrains.annotations.NotNull;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.util.HashSet;
import java.util.Set;

class UpdateFacetsIndex implements CacheEntryProcessor<FacetKey, Set<Facet>, Boolean> {
    @NotNull
    private final Set<Facet> facets;

    public UpdateFacetsIndex(@NotNull Set<Facet> facets) {
        this.facets = facets;
    }

    @Override
    public Boolean process(MutableEntry<FacetKey, Set<Facet>> entry, Object... arguments) throws EntryProcessorException {
        Timer.Context timer = MetricsFactoryImpl.getTimer("update_facets").time();

        Set<Facet> facets = entry.getValue();

        if (facets == null) {
            facets = new HashSet<>();
        }

        facets.addAll(this.facets);

        entry.setValue(facets);

        timer.stop();

        return true;
    }
}
