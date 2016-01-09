package com.fnklabs.draenei.analytics.search;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory for retrieving clustering algorithm
 */
final class ClusteringAlgorithmFactory implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusteringAlgorithmFactory.class);

    /**
     * Clustering algorithms map
     */
    private final Map<Class<? extends ClusteringAlgorithm>, ClusteringAlgorithm> algorithms = new HashMap<>();


    public ClusteringAlgorithmFactory() {
    }

    public <T extends ClusteringAlgorithm> void registerClusteringAlgorithm(T clusteringAlgorithm) {
        getAlgorithms().put(clusteringAlgorithm.getClass(), clusteringAlgorithm);
    }

    /**
     * Get clustering algorithm
     *
     * @param algorithm Clustering algorithm name
     *
     * @return Clustering algorithm
     */
    @NotNull
    public <T extends ClusteringAlgorithm> T get(@NotNull Class<T> algorithm) throws UnknownAlgorithmException {
        T clusteringAlgorithm = (T) getAlgorithms().get(algorithm);

        if (clusteringAlgorithm == null) {
            LOGGER.warn("Requested unknown algorithm: {} available algorithms: {}", algorithm, getAlgorithms().keySet());
            throw new UnknownAlgorithmException(algorithm.getName());
        }

        return clusteringAlgorithm;
    }

    private Map<Class<? extends ClusteringAlgorithm>, ClusteringAlgorithm> getAlgorithms() {
        return algorithms;
    }
}
