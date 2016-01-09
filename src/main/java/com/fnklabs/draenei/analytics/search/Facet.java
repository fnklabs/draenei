package com.fnklabs.draenei.analytics.search;

import com.google.common.base.MoreObjects;
import org.jetbrains.annotations.NotNull;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Facet interface that used by {@link ClusteringAlgorithm} to build facets from content
 * <p>
 * Facet can be any document property by which can be calculated similarity
 */
public class Facet implements Externalizable {

    private FacetKey key;

    private double rank;

    private long document;

    public Facet() {
    }

    /**
     * @param key
     * @param rank
     * @param document
     */
    public Facet(@NotNull FacetKey key, double rank, long document) {
        this.key = key;
        this.rank = rank;
        this.document = document;
    }

    public long getDocument() {
        return document;
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
    public FacetKey getKey() {
        return key;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("key", getKey())
                          .add("rank", getRank())
                          .toString();
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(getKey());
        out.writeDouble(getRank());
        out.writeLong(getDocument());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        key = (FacetKey) in.readObject();
        rank = in.readDouble();
        document = in.readLong();
    }
}
