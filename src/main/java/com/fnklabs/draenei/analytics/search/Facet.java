package com.fnklabs.draenei.analytics.search;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

public class Facet implements Serializable {

    @NotNull
    private final FacetType facetType;

    @NotNull
    private final Serializable value;

    /**
     * @param facetType Facet type (axis)
     * @param value     Facet value
     */
    public Facet(@NotNull FacetType facetType, @NotNull Serializable value) {
        this.facetType = facetType;
        this.value = value;
    }

    /**
     * Check whether specified facets are equals
     *
     * @param left  Facet
     * @param right Facet
     *
     * @return
     */
    @Deprecated
    public static boolean same(@NotNull Facet left, @NotNull Facet right) {
        if (left.getFacetType().getName().equals(FacetType.UNKNOWN) || right.getFacetType().getName().equals(FacetType.UNKNOWN)) {
            return left.getValue().equals(right.getValue());
        }

        return left.equals(right);
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
        if (obj instanceof Facet) {
            Facet facet = (Facet) obj;


            return Objects.equal(getFacetType(), facet.getFacetType()) && Objects.equal(getValue(), facet.getValue());
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
