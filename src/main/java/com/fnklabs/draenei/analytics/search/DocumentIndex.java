package com.fnklabs.draenei.analytics.search;

import org.jetbrains.annotations.NotNull;

import java.util.Set;

public class DocumentIndex {
    @NotNull
    private final Document document;

    @NotNull
    private final Set<Facet> facets;

    public DocumentIndex(@NotNull Document document, @NotNull Set<Facet> facets) {
        this.document = document;
        this.facets = facets;
    }

    @NotNull
    public Document getDocument() {
        return document;
    }

    @NotNull
    public Set<Facet> getFacets() {
        return facets;
    }
}
