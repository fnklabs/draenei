package com.fnklabs.draenei.analytics.search;

import org.jetbrains.annotations.NotNull;

import java.util.Set;

/**
 * Strategy that can create facet map from content
 */
public interface ClusteringAlgorithm {

    /**
     * Build facets from content
     *
     * @param content Content information from which will be build facets
     *
     * @return Set of facets
     */
    @NotNull
    Set<Facet> build(@NotNull Object content);


    /**
     * @param document Content information from which will be build facets
     *
     * @return Set of facets
     */
    Set<Facet> build(@NotNull Document document);
}
