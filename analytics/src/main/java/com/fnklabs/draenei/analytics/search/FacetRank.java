package com.fnklabs.draenei.analytics.search;

import com.google.common.base.MoreObjects;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Facet interface that used by {@link ClusteringAlgorithm} to build facets from content
 * <p>
 * Facet can be any document property by which can be calculated similarity
 */
public class FacetRank implements Externalizable, Comparable<FacetRank> {

    private Facet key;

    private double rank;

    private long document;

    public FacetRank() {
    }

    /**
     * @param key      Facet key
     * @param rank     Facet rank
     * @param document Document to which be facet
     */
    public FacetRank(Facet key, double rank, long document) {
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


    public Facet getKey() {
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
        key = (Facet) in.readObject();
        rank = in.readDouble();
        document = in.readLong();
    }

    @Override
    public int compareTo(FacetRank o) {
        return Double.compare(o.getRank(), getRank());
    }
}
