package com.fnklabs.draenei.analytics;

import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory for retrieving similarity algorithm
 */
@Service
final class SimilarityAlgorithmFactory {

    /**
     * Similarity algorithms map
     */
    private final Map<String, SimilarityAlgorithm> algorithms = new HashMap<>();

    @Autowired
    public SimilarityAlgorithmFactory(List<SimilarityAlgorithm> similarityAlgorithmList) {
        similarityAlgorithmList.forEach(algorithm -> {
            getAlgorithms().put(algorithm.getClass().getName(), algorithm);
        });
    }

    /**
     * Get similarity algorithm
     *
     * @param algorithm Similarity algorithm
     *
     * @return SimilarityAlgorithm instance
     */
    @NotNull
    public SimilarityAlgorithm get(@NotNull String algorithm) {
        if (!getAlgorithms().containsKey(algorithm)) {
            LoggerFactory.getLogger(SimilarityAlgorithmFactory.class).warn("Requested unknown algorithm: {} available algorithms: {}", algorithm, getAlgorithms().keySet());
            throw new UnknownAlgorithm(algorithm);
        }

        return getAlgorithms().get(algorithm);
    }

    private Map<String, SimilarityAlgorithm> getAlgorithms() {
        return algorithms;
    }
}
