package com.fnklabs.draenei.analytics.search;


import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.Objects;

/**
 * Document facet type
 */
final public class FacetType implements Serializable {
    /**
     * Stub facet name for text facets
     */
    public static final String UNKNOWN = "unknown";

    /**
     * Facet name (Document field name)
     */

    private final String name;

    /**
     * Facet value class type (document field class type)
     */

    private final Class type;

    public FacetType(String name, Class type) {
        this.name = name;
        this.type = type;
    }


    public String getName() {
        return name;
    }


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
                          .add("type", getType().getName())
                          .toString();
    }
}
