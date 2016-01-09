package com.fnklabs.draenei.analytics.search;

import com.fnklabs.draenei.MetricsFactoryImpl;
import com.fnklabs.draenei.analytics.TextUtils;
import com.fnklabs.draenei.analytics.morphology.Language;
import com.fnklabs.draenei.orm.CacheableDataProvider;
import com.google.common.util.concurrent.Futures;
import org.apache.lucene.morphology.english.EnglishLuceneMorphology;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ClusteringTfAlgorithmTest {


    @Test
    public void testBuild() throws Exception {
        HashMap<String, String> map = new HashMap<>();
        map.put(Language.EN.name(), EnglishLuceneMorphology.class.getName());
        map.put(Language.RU.name(), RussianLuceneMorphology.class.getName());

        CacheableDataProvider stopWordsDao = Mockito.mock(CacheableDataProvider.class);
        Mockito.when(stopWordsDao.findOneAsync(Matchers.anyString())).thenReturn(Futures.immediateFuture(null));

        TextUtils textUtils = new TextUtils(new MetricsFactoryImpl());


        ClusteringTfAlgorithm algorithm = new ClusteringTfAlgorithm(textUtils);
        Set<Facet> facets = algorithm.build(new TestObject("заголовок", Arrays.asList("текст", "текст")));

        Assert.assertNotNull(facets);
        Assert.assertFalse(facets.isEmpty());

        long titleCount = facets.stream().filter(item -> item.getKey().getFacetType().getName().equals("title")).count();

        Assert.assertEquals(1, titleCount);

        long genresCount = facets.stream().filter(item -> item.getKey().getFacetType().getName().equals("genres")).count();

        Assert.assertEquals(1, genresCount);

        LoggerFactory.getLogger(getClass()).debug("Result: {}", facets);

        Facet titleFacet = facets.stream().filter(item -> item.getKey().getFacetType().getName().equals("title")).findFirst().get();

        Assert.assertEquals("заголовок", titleFacet.getKey().getValue());
        Assert.assertEquals("title", titleFacet.getKey().getFacetType().getName());
        Assert.assertEquals(String.class, titleFacet.getKey().getFacetType().getType());
        Assert.assertEquals(0.33, titleFacet.getRank(), 0.0034);


        Facet genreFacet = facets.stream().filter(item -> item.getKey().getFacetType().getName().equals("genres")).findFirst().get();

        Assert.assertEquals("текст", genreFacet.getKey().getValue());
        Assert.assertEquals("genres", genreFacet.getKey().getFacetType().getName());
        Assert.assertEquals(String.class, genreFacet.getKey().getFacetType().getType());
        Assert.assertEquals(0.66, genreFacet.getRank(), 0.0067);
    }

    @Test
    public void testBuildCase2() throws Exception {
        HashMap<String, String> map = new HashMap<>();
        map.put(Language.EN.name(), EnglishLuceneMorphology.class.getName());
        map.put(Language.RU.name(), RussianLuceneMorphology.class.getName());

        CacheableDataProvider stopWordsDao = Mockito.mock(CacheableDataProvider.class);
        Mockito.when(stopWordsDao.findOneAsync(Matchers.anyString())).thenReturn(Futures.immediateFuture(null));

        TextUtils textUtils = new TextUtils(new MetricsFactoryImpl());


        ClusteringTfAlgorithm algorithm = new ClusteringTfAlgorithm(textUtils);
        Set<Facet> facets = algorithm.build(new TestObject("текст", Arrays.asList("текст")));

        Assert.assertNotNull(facets);
        Assert.assertFalse(facets.isEmpty());

        long titleCount = facets.stream().filter(item -> item.getKey().getFacetType().getName().equals("title")).count();

        Assert.assertEquals(1, titleCount);

        long genresCount = facets.stream().filter(item -> item.getKey().getFacetType().getName().equals("genres")).count();

        Assert.assertEquals(1, genresCount);

        LoggerFactory.getLogger(getClass()).debug("Result: {}", facets);

        Facet titleFacet = facets.stream().filter(item -> item.getKey().getFacetType().getName().equals("title")).findFirst().get();

        Assert.assertEquals("текст", titleFacet.getKey().getValue());
        Assert.assertEquals("title", titleFacet.getKey().getFacetType().getName());
        Assert.assertEquals(String.class, titleFacet.getKey().getFacetType().getType());
        Assert.assertEquals(0.5, titleFacet.getRank(), 0);


        Facet genreFacet = facets.stream().filter(item -> item.getKey().getFacetType().getName().equals("genres")).findFirst().get();

        Assert.assertEquals("текст", genreFacet.getKey().getValue());
        Assert.assertEquals("genres", genreFacet.getKey().getFacetType().getName());
        Assert.assertEquals(String.class, genreFacet.getKey().getFacetType().getType());
        Assert.assertEquals(0.5, genreFacet.getRank(), 0);
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