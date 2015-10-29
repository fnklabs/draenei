package com.fnklabs.draenei.analytics;

import org.jetbrains.annotations.NotNull;

class TfIdfResult extends Facet {
    @NotNull
    private final Object item;

    public TfIdfResult(@NotNull Object item, Facet facet, double rank) {
        super(facet.getType(), facet.getValue(), rank);
        this.item = item;
    }

    @NotNull
    public Object getItem() {
        return item;
    }
}
