package com.fnklabs.draenei.analytics.search;

import java.util.List;

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

    List<Facet> build(Object content);
}
