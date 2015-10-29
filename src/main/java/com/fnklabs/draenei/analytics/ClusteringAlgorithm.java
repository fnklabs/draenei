package com.fnklabs.draenei.analytics;

import org.jetbrains.annotations.NotNull;
import tv.nemo.content.entity.ContentInformation;

import java.util.Set;

/**
 * Strategy that can create facet map from content
 */
interface ClusteringAlgorithm {

    /**
     * Build facets from content
     *
     * @param platformContent Content information from which will be build facets
     *
     * @return Set of facets
     */
    @NotNull
    Set<Facet> build(@NotNull ContentInformation platformContent);

    /**
     * Build facets from text for search
     *
     * @param content Content information from which wil be build facets
     *
     * @return Set of facets
     */
    @NotNull
    Set<Facet> build(@NotNull String content);
}
