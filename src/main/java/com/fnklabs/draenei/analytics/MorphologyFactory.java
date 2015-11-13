package com.fnklabs.draenei.analytics;

import org.apache.lucene.morphology.EnglishLuceneMorphology;
import org.apache.lucene.morphology.Morphology;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class MorphologyFactory {
    public static final Morphology RUSSIAN_LUCENE_MORPHOLOGY;
    public static final Morphology ENGLISH_LUCENE_MORPHOLOGY;
    public static final Morphology MORPHOLOGY = new Morphology() {
        @Override
        public List<String> getNormalForms(String s) {
            return Collections.<String>emptyList();
        }

        @Override
        public List<String> getMorphInfo(String s) {
            return Collections.<String>emptyList();
        }
    };
    private final Map<Language, Morphology> languageMorphology = new HashMap<>();

    public void registerMorphology(@NotNull Language language, @NotNull Morphology morphology) {
        languageMorphology.put(language, morphology);
    }

    @NotNull
    public Morphology getMorphology(Language language) {
        return languageMorphology.getOrDefault(language, MORPHOLOGY);
    }

    public enum Language {
        EN,
        RU
    }

    @NotNull
    private static Morphology getRussianMorphology() {
        try {
            return new RussianLuceneMorphology();
        } catch (IOException e) {
            return MORPHOLOGY;
        }
    }

    @NotNull
    private static Morphology getEnglishMorphology() {
        try {
            return new EnglishLuceneMorphology();
        } catch (IOException e) {
            return MORPHOLOGY;
        }
    }

    static {
        RUSSIAN_LUCENE_MORPHOLOGY = getRussianMorphology();
        ENGLISH_LUCENE_MORPHOLOGY = getEnglishMorphology();
    }
}
