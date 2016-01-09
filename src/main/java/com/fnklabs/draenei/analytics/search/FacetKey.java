package com.fnklabs.draenei.analytics.search;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

class FacetKey implements Serializable {
    private static final long serialVersionUID = -2602992921672567981L;

    @NotNull
    private final FacetType facetType;

    @NotNull
    private final Serializable value;


    public FacetKey(@NotNull FacetType facetType, @NotNull Serializable value) {
        this.facetType = facetType;
        this.value = value;
    }

    @NotNull
    public FacetType getFacetType() {
        return facetType;
    }

    @NotNull
    public Serializable getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getFacetType(), getValue());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FacetKey) {
            FacetKey facetKey = (FacetKey) obj;
            return Objects.equal(getFacetType(), facetKey.getFacetType()) && Objects.equal(getValue(), facetKey.getValue());
        }

        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("facetType", getFacetType())
                          .add("value", getValue())
                          .toString();
    }
}
