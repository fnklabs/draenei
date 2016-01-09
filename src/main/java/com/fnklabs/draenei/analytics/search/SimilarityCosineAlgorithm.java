package com.fnklabs.draenei.analytics.search;

import com.codahale.metrics.Timer;
import com.fnklabs.draenei.MetricsFactory;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

final class SimilarityCosineAlgorithm implements SimilarityAlgorithm {

    private final MetricsFactory metricsFactory;

    SimilarityCosineAlgorithm(MetricsFactory metricsFactory) {
        this.metricsFactory = metricsFactory;
    }

    @Override
    public <T extends Facet, K extends Facet> double getSimilarity(@NotNull Collection<T> firstVector, @NotNull Collection<K> secondVector) {
        Timer.Context time = metricsFactory.getTimer(MetricsType.COSINE_SIMILARITY_GET_SIMILARITY).time();

        double firstVectorModule = getVectorModule(firstVector);
        double secondVectorModule = getVectorModule(secondVector);

        if (firstVectorModule == 0 || secondVectorModule == 0) {
            return 0;
        }


        double scalarVectorComposition = getScalarComposition(firstVector, secondVector);

        double similarity = scalarVectorComposition / (firstVectorModule * secondVectorModule);

        time.stop();

        return similarity;
    }

    protected <T extends Facet, K extends Facet> double getVectorModule(@NotNull Collection<T> firstVector) {
        Timer.Context time = metricsFactory.getTimer(MetricsType.COSINE_SIMILARITY_GET_VECTOR_MODULE).time();

        double vectorPointSum = firstVector.stream()
                                           .mapToDouble(entry -> Math.pow(entry.getRank(), 2))
                                           .sum();

        double sqrt = Math.sqrt(vectorPointSum);

        time.stop();

        return sqrt;
    }

    protected <T extends Facet, K extends Facet> double getScalarComposition(@NotNull Collection<T> firstVector, @NotNull Collection<K> secondVector) {
        Timer.Context time = metricsFactory.getTimer(MetricsType.COSINE_SIMILARITY_GET_SCALAR_COMPOSITION).time();

        Map<FacetKey, K> secondMap = transformToMap(secondVector);

        double sum = 0;

        for (T entry : firstVector) {
//            if (secondMap.containsKey(entry)) {
            K orDefault = secondMap.get(entry.getKey());
            if (orDefault != null) {
//                K orDefault = secondMap.get(entry);
                sum += entry.getRank() * orDefault.getRank();
            }
        }

        time.stop();

        return sum;
    }

    private <T extends Facet> Map<FacetKey, T> transformToMap(@NotNull Collection<T> vector) {
        Timer.Context time = metricsFactory.getTimer(MetricsType.COSINE_SIMILARITY_TRANSFORM_MAP).time();

        Map<FacetKey, T> map = new HashMap<>();

        for (T item : vector) {
            map.putIfAbsent(item.getKey(), item);
        }

        time.stop();
        return map;

    }

    private enum MetricsType implements MetricsFactory.Type {
        COSINE_SIMILARITY_GET_VECTOR_MODULE,
        COSINE_SIMILARITY_GET_SCALAR_COMPOSITION,
        COSINE_SIMILARITY_TRANSFORM_MAP,
        COSINE_SIMILARITY_GET_SIMILARITY

    }
}
