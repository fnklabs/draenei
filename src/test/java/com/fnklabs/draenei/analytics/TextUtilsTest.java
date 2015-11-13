package com.fnklabs.draenei.analytics;

import com.fnklabs.draenei.Metrics;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.Set;

public class TextUtilsTest {

    private MorphologyFactory morphologyFactory;
    private StopWordsDao stopWordsDao;

    @Before
    public void setUp() throws Exception {
        morphologyFactory = new MorphologyFactory();
        morphologyFactory.registerMorphology(MorphologyFactory.Language.RU, MorphologyFactory.RUSSIAN_LUCENE_MORPHOLOGY);
        morphologyFactory.registerMorphology(MorphologyFactory.Language.EN, MorphologyFactory.ENGLISH_LUCENE_MORPHOLOGY);

        SettableFuture<StopWord> stopWordSettableFuture = SettableFuture.<StopWord>create();
        stopWordSettableFuture.set(null);

        stopWordsDao = Mockito.mock(StopWordsDao.class);
        Mockito.when(stopWordsDao.findOneAsync(Matchers.anyString())).thenReturn(stopWordSettableFuture);
    }

    @Test
    public void testGetNormalForms() throws Exception {
        TextUtils textUtils = new TextUtils(morphologyFactory, stopWordsDao, new Metrics());

        Set<String> normalForms = textUtils.getNormalForms("язык", MorphologyFactory.Language.RU);

        Assert.assertNotNull(normalForms);

        normalForms = textUtils.getNormalForms("язык", MorphologyFactory.Language.EN);

        Assert.assertNotNull(normalForms);
    }

    @Test
    public void testTokenizeText() throws Exception {

    }

    @Test
    public void testIsNormalWord() throws Exception {

    }

    @Test
    public void testIsStopWord() throws Exception {

    }
}