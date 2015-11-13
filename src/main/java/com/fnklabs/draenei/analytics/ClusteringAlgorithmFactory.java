package com.fnklabs.draenei.analytics;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory for retrieving clustering algorithm
 */
final class ClusteringAlgorithmFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusteringAlgorithmFactory.class);

    /**
     * Clustering algorithms map
     */
    private final Map<String, ClusteringAlgorithm> algorithms = new HashMap<>();


    public ClusteringAlgorithmFactory(@NotNull List<ClusteringAlgorithm> clusteringAlgorithmList) {
        clusteringAlgorithmList.forEach(algorithm -> {
            getAlgorithms().put(algorithm.getClass().getName(), algorithm);
        });
    }

    /**
     * Get clustering algorithm
     *
     * @param algorithm Clustering algorithm name
     *
     * @return Clustering algorithm
     */
    @NotNull
    public ClusteringAlgorithm get(@NotNull String algorithm) {
        ClusteringAlgorithm clusteringAlgorithm = getAlgorithms().get(algorithm);

        if (clusteringAlgorithm == null) {
            LOGGER.warn("Requested unknown algorithm: {} available algorithms: {}", algorithm, getAlgorithms().keySet());
            throw new UnknownAlgorithm(algorithm);
        }

        return clusteringAlgorithm;
    }

    private Map<String, ClusteringAlgorithm> getAlgorithms() {
        return algorithms;
    }
}
