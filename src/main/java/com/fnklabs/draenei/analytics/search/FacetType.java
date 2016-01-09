package com.fnklabs.draenei.analytics.search;


import com.google.common.base.MoreObjects;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Objects;

/**
 * Facet type of object
 */
final class FacetType implements Serializable {
    /**
     * Facet type name
     */
    @NotNull
    private final String name;

    /**
     * Facet class type
     */
    @NotNull
    private final Class type;

    public FacetType(@NotNull String name, @NotNull Class type) {
        this.name = name;
        this.type = type;
    }

    @NotNull
    public String getName() {
        return name;
    }

    @NotNull
    public Class getType() {
        return type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getType());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FacetType) {
            FacetType facetType = (FacetType) obj;
            return Objects.equals(getName(), facetType.getName())
                    &&
                    Objects.equals(getType(), facetType.getType());
        }

        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("name", getName())
                          .add("type", getType())
                          .toString();
    }
}
