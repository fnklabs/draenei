package com.fnklabs.draenei.analytics.search;

import org.jetbrains.annotations.NotNull;

import java.util.List;
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
    List<Facet> build(@NotNull Object content);
}
