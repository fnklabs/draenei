package com.fnklabs.draenei.analytics;


import org.jetbrains.annotations.NotNull;

import java.util.Collection;

/**
 * Interface for implementing similarity algorithm
 */
interface SimilarityAlgorithm {
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
    <T extends Facet, K extends Facet> double getSimilarity(@NotNull Collection<T> firstFacet, @NotNull Collection<K> secondFacet);


}
