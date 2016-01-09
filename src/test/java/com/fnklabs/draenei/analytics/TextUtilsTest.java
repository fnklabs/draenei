package com.fnklabs.draenei.analytics;

import com.fnklabs.draenei.MetricsFactoryImpl;
import com.fnklabs.draenei.analytics.morphology.Language;
import org.apache.lucene.morphology.english.EnglishLuceneMorphology;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TextUtilsTest {


    @Before
    public void setUp() throws Exception {
        Map<String, String> dictionaries = new HashMap<>();
        dictionaries.put(Language.EN.name(), EnglishLuceneMorphology.class.getName());
        dictionaries.put(Language.RU.name(), RussianLuceneMorphology.class.getName());

    }

    @Test
    public void testGetNormalForms() throws Exception {
        TextUtils textUtils = new TextUtils(new MetricsFactoryImpl());

        Set<String> normalForms = textUtils.getNormalForms("язык", Language.RU);

        Assert.assertNotNull(normalForms);

        normalForms = textUtils.getNormalForms("язык", Language.EN);

        Assert.assertNotNull(normalForms);
    }

    @Test
    public void testExtractWords() throws Exception {
        TextUtils textUtils = new TextUtils(new MetricsFactoryImpl());

        List<String> words = textUtils.extractWords(" тест текст текстовый fff аааа 123", Language.RU);

        LoggerFactory.getLogger(getClass()).debug("Words: {}", words);

        Assert.assertFalse(words.isEmpty());
    }

    @Test
    public void testIsNormalWord() throws Exception {
        TextUtils textUtils = new TextUtils(new MetricsFactoryImpl());

        boolean result = textUtils.isNormalWord("тест", Language.RU);

        Assert.assertTrue(result);

        result = textUtils.isNormalWord("123", Language.RU);

        Assert.assertFalse(result);

        result = textUtils.isNormalWord("", Language.RU);

        Assert.assertFalse(result);
    }


}