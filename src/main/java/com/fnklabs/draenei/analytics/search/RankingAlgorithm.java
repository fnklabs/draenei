package com.fnklabs.draenei.analytics.search;


import java.util.Collection;
import java.util.Set;

/**
 * Algorithm for calculating/build facets rank
 */
public interface RankingAlgorithm {
    /**
     * Calculate facet rank from facets list
     *
     * @param facets Facet collection from which will be calculated rank
     *
     * @return Unique facets rank collection
     */
    Set<FacetRank> calculate(Collection<Facet> facets);
}
