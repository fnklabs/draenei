package com.fnklabs.draenei.analytics.search;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public interface SearchService extends Serializable {

    void addDocument(@NotNull Document document);

    @NotNull
    List<SearchResult> search(@NotNull Set<Facet> facets);

    @NotNull
    List<SearchResult> getRecommendation(long documentId);

    void rebuildDocumentIndex();

    @NotNull
    List<SearchResult> search(@NotNull String text);

    @NotNull
    Set<Facet> buildFacets(@NotNull Document document);

    @NotNull
    Set<Facet> buildFacets(@NotNull String text);


}
