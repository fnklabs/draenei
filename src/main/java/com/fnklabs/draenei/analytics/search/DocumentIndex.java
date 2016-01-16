package com.fnklabs.draenei.analytics.search;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Set;

public class DocumentIndex implements Serializable {
    @NotNull
    private final Document document;

    @NotNull
    private final Set<FacetRank> facetRanks;

    public DocumentIndex(@NotNull Document document, @NotNull Set<FacetRank> facetRanks) {
        this.document = document;
        this.facetRanks = facetRanks;
    }

    @NotNull
    public Document getDocument() {
        return document;
    }

    @NotNull
    public Set<FacetRank> getFacetRanks() {
        return facetRanks;
    }
}
