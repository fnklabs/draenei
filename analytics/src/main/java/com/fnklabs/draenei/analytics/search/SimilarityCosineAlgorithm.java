package com.fnklabs.draenei.analytics.search;

import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;

import java.util.Collection;
import java.util.Optional;

final public class SimilarityCosineAlgorithm implements SimilarityAlgorithm {


    SimilarityCosineAlgorithm() {

    }

    @Override
    public <T extends FacetRank, K extends FacetRank> double getSimilarity(Collection<T> firstVector, Collection<K> secondVector) {
        Timer time = MetricsFactory.getMetrics().getTimer(MetricsType.COSINE_SIMILARITY_GET_SIMILARITY.name());

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

    protected <T extends FacetRank, K extends FacetRank> double getVectorModule(Collection<T> firstVector) {
        Timer time = MetricsFactory.getMetrics().getTimer(MetricsType.COSINE_SIMILARITY_GET_VECTOR_MODULE.name());

        double vectorPointSum = firstVector.stream()
                                           .mapToDouble(entry -> Math.pow(entry.getRank(), 2))
                                           .sum();

        double sqrt = Math.sqrt(vectorPointSum);

        time.stop();

        return sqrt;
    }

    protected <T extends FacetRank, K extends FacetRank> double getScalarComposition(Collection<T> firstVector, Collection<K> secondVector) {
        Timer time = MetricsFactory.getMetrics().getTimer(MetricsType.COSINE_SIMILARITY_GET_SCALAR_COMPOSITION.name());

        double sum = 0;

        for (T entry : firstVector) {
            Optional<K> first = getVectorAxis(secondVector, entry);

            if (first.isPresent()) {
                sum += entry.getRank() * first.get().getRank();
            }
        }

        time.stop();

        return sum;
    }

    /**
     * @param secondVector Vector for search
     * @param entry        Entry that must be search
     * @param <T>          First vector class type
     * @param <K>          axis class type
     *
     * @return
     */
    private <T extends FacetRank, K extends FacetRank> Optional<K> getVectorAxis(Collection<K> secondVector, T entry) {
        return secondVector.stream()
                           .filter(facet -> {
                               return Facet.same(facet.getKey(), entry.getKey());
                           })
                           .findFirst();
    }

    private enum MetricsType {
        COSINE_SIMILARITY_GET_VECTOR_MODULE,
        COSINE_SIMILARITY_GET_SCALAR_COMPOSITION,
        COSINE_SIMILARITY_TRANSFORM_MAP,
        COSINE_SIMILARITY_GET_SIMILARITY

    }
}
