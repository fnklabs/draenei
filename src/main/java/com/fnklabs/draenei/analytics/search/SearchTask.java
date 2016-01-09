package com.fnklabs.draenei.analytics.search;

import com.codahale.metrics.Timer;
import com.fnklabs.draenei.MetricsFactoryImpl;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

class SearchTask extends ComputeTaskAdapter<Object, List<SearchResult>> {
    /**
     *
     */
    @NotNull
    private final Set<Facet> searchFacets;

    private Ignite ignite;


    public SearchTask(@NotNull Set<Facet> searchFacets) {
        this.searchFacets = searchFacets;
    }

    @Nullable
    @Override
    public Map<SearchJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) throws IgniteException {
        Map<SearchJob, ClusterNode> map = new HashMap<>(subgrid.size());

        subgrid.forEach(clusterNode -> {
            map.put(new SearchJob(searchFacets), clusterNode);
        });

        return map;
    }

    @Nullable
    @Override
    public List<SearchResult> reduce(List<ComputeJobResult> results) throws IgniteException {

        Timer.Context totalTimer = MetricsFactoryImpl.getTimer("search_task.reduce.total").time();

        Set<DocumentIndex> documentIndexes = results.stream()
                                                    .flatMap(computeJobResult -> {
                                                        return computeJobResult.<Set<DocumentIndex>>getData().stream();
                                                    })
                                                    .collect(Collectors.toSet());

        Map<FacetKey, List<Facet>> facetKeyListMap = documentIndexes.stream()
                                                                    .flatMap(documentIndex -> {
                                                                        Set<Facet> facets = documentIndex.getFacets()
                                                                                                         .stream()
                                                                                                         .collect(Collectors.toSet());

                                                                        return facets.stream();
                                                                    })
                                                                    .collect(Collectors.groupingBy(new Function<Facet, FacetKey>() {
                                                                        @Override
                                                                        public FacetKey apply(Facet facet) {
                                                                            return facet.getKey();
                                                                        }
                                                                    }));


        int totalDocuments = ignite.getOrCreateCache(SearchServiceImpl.getDocumentsCacheConfiguration()).size();

        Map<FacetKey, Double> facetIdfMap = new HashMap<>();

        facetKeyListMap.entrySet()
                       .forEach(entry -> {
                           double idf = TfIdfUtils.calculateIdf(entry.getValue().size(), totalDocuments);

                           facetIdfMap.put(entry.getKey(), idf);
                       });

        SimilarityCosineAlgorithm algorithm = new SimilarityCosineAlgorithm(MetricsFactoryImpl.getInstance());

        List<Facet> searchFacetTfIdf = searchFacets.stream()
                                                   .map(facet -> {
                                                       FacetKey facetKey = facet.getKey();
                                                       double tfIdf = TfIdfUtils.calculateTfIdf(facet.getRank(), facetIdfMap.getOrDefault(facetKey, 0d));
                                                       return new Facet(facetKey, tfIdf, facet.getDocument());
                                                   })
                                                   .collect(Collectors.toList());
        Timer.Context mapDocumentsTimer = MetricsFactoryImpl.getTimer("search_task.reduce.documents_map").time();

        List<SearchResult> searchResults = documentIndexes.stream()
                                                          .map(documentIndex -> {
                                                              Set<Facet> tfIdfFacets = documentIndex.getFacets()
                                                                                                    .stream()
                                                                                                    .map(facet -> {
                                                                                                        FacetKey facetKey = facet.getKey();
                                                                                                        double tfIdf = TfIdfUtils.calculateTfIdf(facet.getRank(), facetIdfMap.getOrDefault(facetKey, 0d));
                                                                                                        return new Facet(facetKey, tfIdf, facet.getDocument());
                                                                                                    })
                                                                                                    .collect(Collectors.toSet());


                                                              double similarity = algorithm.getSimilarity(tfIdfFacets, searchFacetTfIdf);

                                                              return new SearchResult(documentIndex.getDocument(), similarity);
                                                          })
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
