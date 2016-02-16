package com.fnklabs.draenei.analytics.search;


import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;

public class SimilarityCosineAlgorithmTest {

    @Test
    public void testGetSimilarity() throws Exception {

    }

    @Test
    public void testGetVectorModule() throws Exception {

    }

    @Test
    public void testGetScalarComposition() throws Exception {
        SimilarityCosineAlgorithm similarityCosineAlgorithm = new SimilarityCosineAlgorithm();

        FacetType facetType = new FacetType(FacetType.UNKNOWN, String.class);

        HashSet<FacetRank> firstVector = Sets.newHashSet(new FacetRank(new Facet(facetType, "text"), 1, 0));
        HashSet<FacetRank> secondVector = Sets.newHashSet(new FacetRank(new Facet(facetType, "text"), 1, 0));

        double scalarComposition = similarityCosineAlgorithm.getScalarComposition(firstVector, secondVector);

        Assert.assertNotEquals(0, scalarComposition, 0);

        Sets.newHashSet(new FacetRank(new Facet(facetType, "text"), 1, 0));
        Sets.newHashSet(new FacetRank(new Facet(new FacetType("field", String.class), "text"), 1, 0));

        scalarComposition = similarityCosineAlgorithm.getScalarComposition(firstVector, secondVector);

        Assert.assertNotEquals(0, scalarComposition, 0);
    }
}