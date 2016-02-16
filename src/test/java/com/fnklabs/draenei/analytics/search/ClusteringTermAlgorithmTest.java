package com.fnklabs.draenei.analytics.search;


import com.fnklabs.draenei.analytics.TextUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class ClusteringTermAlgorithmTest {


    @Test
    public void testBuild() throws Exception {
        TextUtils textUtils = new TextUtils();


        ClusteringTermAlgorithm algorithm = new ClusteringTermAlgorithm(textUtils);
        List<Facet> facets = algorithm.build(new TestObject("заголовок", Arrays.asList("текст", "текст")));

        Assert.assertNotNull(facets);
        Assert.assertFalse(facets.isEmpty());

        long titleCount = facets.stream().filter(item -> item.getFacetType().getName().equals("title")).count();

//        Assert.assertEquals(1, titleCount);

        long genresCount = facets.stream().filter(item -> item.getFacetType().getName().equals("genres")).count();

        Assert.assertEquals(2, genresCount);

        LoggerFactory.getLogger(getClass()).debug("Result: {}", facets);

        Facet titleFacetRank = facets.stream().filter(item -> item.getFacetType().getName().equals("title")).findFirst().get();

        Assert.assertEquals("заголовок", titleFacetRank.getValue());
        Assert.assertEquals("title", titleFacetRank.getFacetType().getName());
        Assert.assertEquals(String.class, titleFacetRank.getFacetType().getType());
//        Assert.assertEquals(0.33, titleFacetRank.getRank(), 0.0034);


        Facet genreFacetRank = facets.stream().filter(item -> item.getFacetType().getName().equals("genres")).findFirst().get();

        Assert.assertEquals("текст", genreFacetRank.getValue());
        Assert.assertEquals("genres", genreFacetRank.getFacetType().getName());
        Assert.assertEquals(String.class, genreFacetRank.getFacetType().getType());
//        Assert.assertEquals(0.66, genreFacetRank.getRank(), 0.0067);
    }

    @Test
    public void testBuildCase2() throws Exception {
        TextUtils textUtils = new TextUtils();


        ClusteringTermAlgorithm algorithm = new ClusteringTermAlgorithm(textUtils);
        List<Facet> facetRanks = algorithm.build(new TestObject("текст", Arrays.asList("текст")));

        Assert.assertNotNull(facetRanks);
        Assert.assertFalse(facetRanks.isEmpty());

        long titleCount = facetRanks.stream().filter(item -> item.getFacetType().getName().equals("title")).count();

        Assert.assertEquals(1, titleCount);

        long genresCount = facetRanks.stream().filter(item -> item.getFacetType().getName().equals("genres")).count();

        Assert.assertEquals(1, genresCount);

        LoggerFactory.getLogger(getClass()).debug("Result: {}", facetRanks);

        Facet titleFacetRank = facetRanks.stream().filter(item -> item.getFacetType().getName().equals("title")).findFirst().get();

        Assert.assertEquals("текст", titleFacetRank.getValue());
        Assert.assertEquals("title", titleFacetRank.getFacetType().getName());
        Assert.assertEquals(String.class, titleFacetRank.getFacetType().getType());
//        Assert.assertEquals(0.5, titleFacetRank.getRank(), 0);


        Facet genreFacetRank = facetRanks.stream().filter(item -> item.getFacetType().getName().equals("genres")).findFirst().get();

        Assert.assertEquals("текст", genreFacetRank.getValue());
        Assert.assertEquals("genres", genreFacetRank.getFacetType().getName());
        Assert.assertEquals(String.class, genreFacetRank.getFacetType().getType());
//        Assert.assertEquals(0.5, genreFacetRank.getRank(), 0);
    }

    private static class TestObject {
        //        @com.fnklabs.draenei.analytics.search.annotation.Facet
        private UUID id = UUID.randomUUID();

        @com.fnklabs.draenei.analytics.search.annotation.Facet
        private String title;

        @com.fnklabs.draenei.analytics.search.annotation.Facet
        private List<String> genres;

        public TestObject(String title, List<String> genres) {
            this.id = id;
            this.title = title;
            this.genres = genres;
        }

        public UUID getId() {
            return id;
        }

        public void setId(UUID id) {
            this.id = id;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public List<String> getGenres() {
            return genres;
        }

        public void setGenres(List<String> genres) {
            this.genres = genres;
        }
    }
}