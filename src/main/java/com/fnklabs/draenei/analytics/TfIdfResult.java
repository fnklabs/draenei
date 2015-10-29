package com.fnklabs.draenei.analytics;

import org.jetbrains.annotations.NotNull;
import tv.nemo.content.entity.ContentInformation;

class TfIdfResult extends Facet {
    @NotNull
    private final ContentInformation item;

    public TfIdfResult(@NotNull ContentInformation item, Facet facet, double rank) {
        super(facet.getType(), facet.getValue(), rank);
        this.item = item;
    }

    @NotNull
    public ContentInformation getItem() {
        return item;
    }
}
