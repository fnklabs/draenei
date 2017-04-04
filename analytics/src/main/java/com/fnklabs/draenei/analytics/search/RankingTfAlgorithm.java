package com.fnklabs.draenei.analytics.search;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RankingTfAlgorithm implements RankingAlgorithm {
    /**
     * {@inheritDoc}
     *
     * @param facets
     */
    @Override
    public Set<FacetRank> calculate(Collection<Facet> facets) {
        // calculate TF
        Set<FacetRank> facetsRank = facets.stream()
                                          .collect(Collectors.groupingBy(new GroupByFacetType()))
                                          .entrySet()
                                          .stream()
                                          .flatMap(facetTypeEntry -> {
                                              List<Facet> valuesByFacetType = facetTypeEntry.getValue();
                                              int termsByFacetType = valuesByFacetType.size();

                                              return valuesByFacetType.stream()
                                                                      .collect(Collectors.groupingBy(new GroupByFacet()))
                                                                      .entrySet()
                                                                      .stream()
                                                                      .flatMap(new CalculateTfFunction(termsByFacetType));
                                          })
                                          .collect(Collectors.toSet());

        return facetsRank;
    }

    private static class CalculateTfFunction implements Function<Map.Entry<Facet, List<Facet>>, Stream<FacetRank>> {
        private final int termsFacetType;

        private CalculateTfFunction(int termsFacetType) {
            this.termsFacetType = termsFacetType;
        }


        @Override
        public Stream<FacetRank> apply(Map.Entry<Facet, List<Facet>> entry) {
            double rank = TfIdfUtils.calculateTf(entry.getValue().size(), termsFacetType);

            return Stream.of(new FacetRank(entry.getKey(), rank, 0));
        }
    }

    private static class GroupByFacetType implements Function<Facet, FacetType> {

        @Override
        public FacetType apply(Facet facet) {
            return facet.getFacetType();
        }
    }

    private static class GroupByFacet implements Function<Facet, Facet> {

        @Override
        public Facet apply(Facet facet) {
            return facet;
        }
    }

}
