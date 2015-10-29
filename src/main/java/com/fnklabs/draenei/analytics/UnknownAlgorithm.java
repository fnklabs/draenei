package com.fnklabs.draenei.analytics;

/**
 * Unknown algorithm exception. Used by {@link ClusteringAlgorithmFactory} && {@link SimilarityAlgorithmFactory}
 */
class UnknownAlgorithm extends RuntimeException {

    /**
     * @param algorithm Algorithm name
     */
    public UnknownAlgorithm(String algorithm) {
        super(String.format("Unknown algorithm: [%s]", algorithm));
    }
}
