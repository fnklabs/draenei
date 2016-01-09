package com.fnklabs.draenei.analytics.search;

import com.fnklabs.draenei.analytics.search.ClusteringAlgorithmFactory;
import com.fnklabs.draenei.analytics.search.SimilarityAlgorithmFactory;

/**
 * Unknown algorithm exception. Used by {@link ClusteringAlgorithmFactory} and {@link SimilarityAlgorithmFactory}
 */
class UnknownAlgorithmException extends RuntimeException {

    /**
     * @param algorithm Algorithm name
     */
    public UnknownAlgorithmException(String algorithm) {
        super(String.format("Unknown algorithm: [%s]", algorithm));
    }
}
