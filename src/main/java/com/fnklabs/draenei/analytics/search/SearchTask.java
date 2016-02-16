package com.fnklabs.draenei.analytics.search;

import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SearchTask<T extends Predicate<DocumentIndex> & Serializable> extends ComputeTaskAdapter<Object, List<SearchResult>> {
    /**
     *
     */
    @NotNull
    private final Collection<FacetRank> searchFacetRanks;

    private Ignite ignite;

    private final T userPredicate;


    public SearchTask(@NotNull Collection<FacetRank> searchFacetRanks, T userPredicate) {
        this.searchFacetRanks = searchFacetRanks;
        this.userPredicate = userPredicate;
    }

    @Nullable
    @Override
    public Map<SearchJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) throws IgniteException {
        Map<SearchJob, ClusterNode> map = new HashMap<>(subgrid.size());

        subgrid.forEach(clusterNode -> {
            map.put(new SearchJob<T>(searchFacetRanks, userPredicate), clusterNode);
        });

        return map;
    }

    @Nullable
    @Override
    public List<SearchResult> reduce(List<ComputeJobResult> results) throws IgniteException {

        Timer totalTimer = MetricsFactory.getMetrics().getTimer("search_task.reduce.total");

        Set<DocumentIndex> documentIndexes = results.stream()
                                                    .flatMap(computeJobResult -> {
                                                        return computeJobResult.<Set<DocumentIndex>>getData().stream();
                                                    })
                                                    .collect(Collectors.toSet());

        Map<Facet, List<FacetRank>> facetKeyListMap = documentIndexes.stream()
                                                                     .flatMap(documentIndex -> {
                                                                         Set<FacetRank> facetRanks = documentIndex.getFacetRanks()
                                                                                                                  .stream()
                                                                                                                  .collect(Collectors.toSet());

                                                                         return facetRanks.stream();
                                                                     })
                                                                     .collect(Collectors.groupingBy(new Function<FacetRank, Facet>() {
                                                                         @Override
                                                                         public Facet apply(FacetRank facet) {
                                                                             return facet.getKey();
                                                                         }
                                                                     }));


        int totalDocuments = ignite.getOrCreateCache(DraeneiSearchService.getDocumentsCacheConfiguration()).size();

        Map<Facet, Double> facetIdfMap = new HashMap<>();

        facetKeyListMap.entrySet()
                       .forEach(entry -> {
                           double idf = TfIdfUtils.calculateIdf(entry.getValue().size(), totalDocuments);

                           facetIdfMap.put(entry.getKey(), idf);
                       });

        SimilarityCosineAlgorithm algorithm = new SimilarityCosineAlgorithm();

        List<FacetRank> searchFacetRankTfIdf = searchFacetRanks.stream()
                                                               .map(facet -> {
                                                                   Facet facetKey = facet.getKey();
                                                                   Double searchIdf = TfIdfUtils.calculateIdf(1, searchFacetRanks.size());//facetIdfMap.getOrDefault(facetKey, 1d);
                                                                   double tfIdf = TfIdfUtils.calculateTfIdf(facet.getRank(), searchIdf);
                                                                   return new FacetRank(facetKey, tfIdf, facet.getDocument());
                                                               })
                                                               .collect(Collectors.toList());
        Timer mapDocumentsTimer = MetricsFactory.getMetrics().getTimer("search_task.reduce.documents_map");

        List<SearchResult> searchResults = documentIndexes.stream()
                                                          .map(documentIndex -> {
                                                              Set<FacetRank> tfIdfFacetRanks = documentIndex.getFacetRanks()
                                                                                                            .stream()
                                                                                                            .map(facet -> {
                                                                                                                Facet facetKey = facet.getKey();
                                                                                                                double tfIdf = TfIdfUtils.calculateTfIdf(facet.getRank(), facetIdfMap
                                                                                                                        .getOrDefault(facetKey, 0d));
                                                                                                                return new FacetRank(facetKey, tfIdf, facet.getDocument());
                                                                                                            })
                                                                                                            .collect(Collectors.toSet());


                                                              double similarity = algorithm.getSimilarity(tfIdfFacetRanks, searchFacetRankTfIdf);

                                                              return new SearchResult(documentIndex.getDocument(), similarity);
                                                          })
                                                          .filter(searchResult -> searchResult.getRank() > 0)
                                                          .sorted()
                                                          .collect(Collectors.toList());

        mapDocumentsTimer.stop();
        totalTimer.stop();

        return searchResults;
    }

    @IgniteInstanceResource
    protected void setIgnite(Ignite ignite) {
        this.ignite = ignite;
    }
}
