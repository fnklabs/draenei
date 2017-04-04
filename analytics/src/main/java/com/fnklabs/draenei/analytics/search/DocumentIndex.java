package com.fnklabs.draenei.analytics.search;

import java.io.Serializable;
import java.util.Set;

public class DocumentIndex implements Serializable {

    private final Document document;


    private final Set<FacetRank> facetRanks;

    public DocumentIndex(Document document, Set<FacetRank> facetRanks) {
        this.document = document;
        this.facetRanks = facetRanks;
    }


    public Document getDocument() {
        return document;
    }


    public Set<FacetRank> getFacetRanks() {
        return facetRanks;
    }
}
