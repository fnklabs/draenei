package com.fnklabs.draenei.analytics;

import com.google.common.base.Objects;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * Facet interface that used by {@link ClusteringAlgorithm} to build facets from content
 * <p>
 * Facet can be any document property by which can be calculated similarity
 */
class Facet implements Serializable {

    @NotNull
    private final Key key;

    private final double rank;

    Facet(@NotNull FacetType facetType, @NotNull Serializable value, double rank) {
        this.key = new Key(facetType, value);
        this.rank = rank;
    }

    /**
     * Return facet name.
     * <p>
     * Clustering name must override equals method to provider normal functionality for comparing two clusters
     *
     * @return Clustering name
     */
    @NotNull
    public FacetType getType() {
        return key.getFacetType();
    }

    @NotNull
    public Serializable getValue() {
        return key.getValue();
    }

    /**
     * Return facet value.
     * <p>
     * Higher value means higher weight
     *
     * @return clustering value
     */
    public double getRank() {
        return rank;
    }

    @NotNull
    public Key getKey() {
        return key;
    }

    public static class Key implements Serializable {
        private static final long serialVersionUID = -2602992921672567981L;

        @NotNull
        private final FacetType facetType;

        @NotNull
        private final Serializable value;


        private Key(@NotNull FacetType facetType, @NotNull Serializable value) {
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
            if (obj instanceof Facet.Key) {
                return Objects.equal(getFacetType(), ((Facet.Key) obj).getFacetType()) && Objects.equal(getValue(), ((Facet.Key) obj).getValue());
            }

            return false;
        }

        @Override
        public String toString() {
            return super.toString();
        }
    }


}
