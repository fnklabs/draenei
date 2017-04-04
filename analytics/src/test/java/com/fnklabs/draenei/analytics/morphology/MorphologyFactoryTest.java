package com.fnklabs.draenei.analytics.morphology;

import org.apache.lucene.morphology.Morphology;
import org.apache.lucene.morphology.english.EnglishLuceneMorphology;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.junit.Assert;
import org.junit.Test;

public class MorphologyFactoryTest {

    @Test
    public void testGetMorphology() throws Exception {


        Morphology morphology = MorphologyFactory.getMorphology(Language.EN);

        Assert.assertNotNull(morphology);

        Assert.assertEquals(morphology.getClass(), EnglishLuceneMorphology.class);

        morphology = MorphologyFactory.getMorphology(Language.RU);

        Assert.assertNotNull(morphology);

        Assert.assertEquals(morphology.getClass(), RussianLuceneMorphology.class);
    }
}