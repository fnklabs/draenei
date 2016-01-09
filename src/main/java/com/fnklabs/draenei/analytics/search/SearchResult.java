package com.fnklabs.draenei.analytics.search;

import org.jetbrains.annotations.NotNull;

public class SearchResult implements Comparable<SearchResult> {
    private final Document document;
    private final double rank;

    public SearchResult(@NotNull Document document, double rank) {
        this.document = document;
        this.rank = rank;
    }

    public Document getDocument() {
        return document;
    }

    public double getRank() {
        return rank;
    }

    @Override
    public int compareTo(@NotNull SearchResult o) {
        return Double.compare(o.getRank(), getRank());
    }
}
