package com.fnklabs.draenei.analytics.search;


import java.util.Collection;

/**
 * Interface for implementing similarity algorithm
 */
public interface SimilarityAlgorithm {
    /**
     * Get similarity among two specified clusters
     * <p>
     * Higher value means higher similarity.
     *
     * @param firstFacet  First cluster
     * @param secondFacet Second cluster
     *
     * @return Similarity index
     */
    <T extends FacetRank, K extends FacetRank> double getSimilarity(Collection<T> firstFacet, Collection<K> secondFacet);
}
